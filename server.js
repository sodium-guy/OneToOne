const express = require('express');
const fs = require('fs');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// public 폴더가 있으면 그걸 쓰고, 없으면 현재 폴더를 씀
const publicDir = fs.existsSync(path.join(__dirname, 'public'))
    ? path.join(__dirname, 'public')
    : __dirname;

app.use(express.static(publicDir));

app.get('/', (req, res) => {
    res.sendFile(path.join(publicDir, 'index.html'));
});

app.get('/admin', (req, res) => {
    res.sendFile(path.join(publicDir, 'admin.html'));
});

app.get('/admin.html', (req, res) => {
    res.sendFile(path.join(publicDir, 'admin.html'));
});


// ---------------------------------------------
// 설정
// ---------------------------------------------

const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'admin1234'; // 배포 시 꼭 바꾸세요

const CHARACTER_PAIRS = [
    ['견우', '직녀'],
    ['선재', '임솔'],
    ['로미오', '줄리엣'],
    ['춘향', '몽룡'],
    ['온달', '평강'],
    ['피터팬', '웬디'],
    ['왕자', '인어공주'],
    ['홍길동', '월매'],
    ['해와', '달이'],
    ['별이', '달이'],
    ['봄이', '여름이'],
    ['가을이', '겨울이'],
    ['하늘', '바다'],
    ['해', '달'],
    ['빛', '그림자'],
];


// ---------------------------------------------
// 상태
// ---------------------------------------------

const devices = new Map();
const matchHistory = new Map();

let currentRound = 0;

let timerState = {
    running: false,
    endTime: null,
    durationSec: null,
};

const admins = new Set();
const adminClientIds = new Set(); // 새로고침 시 어드민 권한 유지를 위한 Client ID 목록


// ---------------------------------------------
// 유틸
// ---------------------------------------------

function shuffle(arr) {
    const a = [...arr];

    for (let i = a.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [a[i], a[j]] = [a[j], a[i]];
    }

    return a;
}


function normalizeClientId(rawClientId, socketId) {
    if (typeof rawClientId === 'string' && rawClientId.trim()) {
        return rawClientId.trim();
    }

    // 클라이언트가 아직 clientId를 안 보내면 임시로 socket.id 사용
    return 'socket:' + socketId;
}


function getIp(socket) {
    const xff = socket.handshake.headers['x-forwarded-for'];

    if (typeof xff === 'string' && xff.trim()) {
        return xff.split(',')[0].trim();
    }

    return socket.handshake.address || '';
}


function hasMatchedBefore(a, b) {
    return matchHistory.get(a)?.has(b) || false;
}


function recordMatch(a, b) {
    if (!matchHistory.has(a)) {
        matchHistory.set(a, new Set());
    }

    if (!matchHistory.has(b)) {
        matchHistory.set(b, new Set());
    }

    matchHistory.get(a).add(b);
    matchHistory.get(b).add(a);
}


function removeFromMatchHistory(id) {
    matchHistory.delete(id);

    matchHistory.forEach((set) => {
        set.delete(id);
    });
}


function getOnlineCount() {
    let count = 0;

    for (const d of devices.values()) {
        if (d.status !== 'inactive') {
            count += 1;
        }
    }

    return count;
}


function broadcastParticipantCount() {
    io.emit('participantCount', getOnlineCount());
}


function broadcastDeviceList() {
    const list = [...devices.values()].map((d) => ({
        id: d.clientId,
        nickname: d.nickname,
        status: d.status,
        ip: d.ip,
    }));

    admins.forEach((adminId) => {
        io.to(adminId).emit('deviceList', list);
    });
}


function cleanupInactiveDevices() {
    for (const [clientId, d] of [...devices.entries()]) {
        if (
            d.status === 'inactive' &&
            typeof d.inactiveRound === 'number' &&
            currentRound - d.inactiveRound >= 2
        ) {
            devices.delete(clientId);
            removeFromMatchHistory(clientId);
        }
    }
}

function buildPairs(idList) {
    let best = null;

    for (let attempt = 0; attempt < 300; attempt++) {
        const shuffled = shuffle(idList);
        const pairs = [];
        const used = new Set();

        for (let i = 0; i < shuffled.length; i++) {
            const id = shuffled[i];

            if (used.has(id)) {
                continue;
            }

            let partner = null;

            for (let j = i + 1; j < shuffled.length; j++) {
                const cand = shuffled[j];

                if (used.has(cand)) {
                    continue;
                }

                if (!hasMatchedBefore(id, cand)) {
                    partner = cand;
                    break;
                }
            }

            if (partner) {
                pairs.push([id, partner]);
                used.add(id);
                used.add(partner);
            }
        }

        const unmatched = shuffled.filter((id) => !used.has(id));

        if (unmatched.length <= 1) {
            return {
                pairs,
                leftover: unmatched[0] || null,
            };
        }

        if (!best || unmatched.length < best.unmatched.length) {
            best = {
                pairs,
                unmatched,
            };
        }
    }

    return {
        pairs: best?.pairs || [],
        leftover: best?.unmatched?.[0] || null,
    };
}


function sendTimerStatus(socket) {
    socket.emit('timerStatus', timerState);
}


function setDeviceInactiveBySocket(socketId) {
    for (const device of devices.values()) {
        if (device.socketId !== socketId) {
            continue;
        }

        // 중복 조건문 제거 완료
        device.resumeStatus =
            device.status === 'inactive'
                ? device.resumeStatus || 'idle'
                : device.status;

        device.status = 'inactive';
        device.inactiveRound = currentRound;

        return true;
    }

    return false;
}


function registerOrRestoreDevice(socket) {
    const clientId = normalizeClientId(
        socket.handshake.auth?.clientId,
        socket.id
    );

    const ip = getIp(socket);

    // 어드민 세션 복구 처리
    if (adminClientIds.has(clientId)) {
        admins.add(socket.id);
    }


    let device = devices.get(clientId);


    if (!device) {
        device = {
            clientId,
            socketId: socket.id,
            ip,

            nickname:
                '참가자' +
                clientId
                    .replace(/[^a-zA-Z0-9]/g, '')
                    .slice(0, 4),

            status: 'idle',
            resumeStatus: 'idle',
            inactiveRound: null,

            character: null,
            partnerCharacter: null,
        };

        devices.set(clientId, device);

        return device;
    }


    device.socketId = socket.id;
    device.ip = ip;

    device.status = device.resumeStatus || 'idle';
    device.inactiveRound = null;


    if (device.status === 'idle') {
        device.character = null;
        device.partnerCharacter = null;
    }


    return device;
}


// ---------------------------------------------
// 소켓 이벤트
// ---------------------------------------------

io.on('connection', (socket) => {

    const device = registerOrRestoreDevice(socket);

    socket.data.clientId = device.clientId;


    socket.emit('connected', {
        id: socket.id,
        clientId: device.clientId,
        nickname: device.nickname,
        status: device.status,
    });


    broadcastDeviceList();
    broadcastParticipantCount();


    // ---- 어드민 로그인 ----

    socket.on('adminLogin', (password, cb) => {

        if (password === ADMIN_PASSWORD) {

            admins.add(socket.id);

            if (device.clientId) {
                adminClientIds.add(device.clientId);
            }

            cb?.({
                ok: true,
            });

            broadcastDeviceList();
            sendTimerStatus(socket);

        } else {

            cb?.({
                ok: false,
                error: '비밀번호가 틀렸습니다.',
            });

        }

    });
    // ---- 매칭 시작 (어드민 전용) ----

    socket.on('startMatching', () => {

        if (!admins.has(socket.id)) {
            return;
        }


        cleanupInactiveDevices();


        const idleIds = [...devices.entries()]
            .filter(([, d]) => d.status === 'idle')
            .map(([id]) => id);


        if (idleIds.length < 2) {

            socket.emit(
                'adminMessage',
                '매칭 가능한 대기 인원이 2명 미만입니다.'
            );

            return;
        }


        currentRound += 1;


        const { pairs, leftover } = buildPairs(idleIds);

        const shuffledCharacters = shuffle(CHARACTER_PAIRS);



        pairs.forEach(([a, b], idx) => {

            const [charA, charB] =
                shuffledCharacters[idx % shuffledCharacters.length];


            recordMatch(a, b);


            const deviceA = devices.get(a);
            const deviceB = devices.get(b);


            if (!deviceA || !deviceB) {
                return;
            }


            deviceA.status = 'matched';
            deviceA.resumeStatus = 'matched';
            deviceA.character = charA;
            deviceA.partnerCharacter = charB;
            deviceA.inactiveRound = null;


            deviceB.status = 'matched';
            deviceB.resumeStatus = 'matched';
            deviceB.character = charB;
            deviceB.partnerCharacter = charA;
            deviceB.inactiveRound = null;



            if (deviceA.socketId) {

                io.to(deviceA.socketId).emit('matched', {
                    round: currentRound,
                    me: charA,
                    partner: charB,
                });

            }



            if (deviceB.socketId) {

                io.to(deviceB.socketId).emit('matched', {
                    round: currentRound,
                    me: charB,
                    partner: charA,
                });

            }

        });



        if (leftover) {

            const leftoverDevice = devices.get(leftover);


            if (leftoverDevice?.socketId) {

                io.to(leftoverDevice.socketId).emit(
                    'matchWaiting',
                    {
                        round: currentRound,
                    }
                );

            }

        }



        const msgText =
            pairs.length +
            '쌍 매칭 완료' +
            (leftover ? ' (1명 대기)' : '');


        socket.emit('adminMessage', msgText);


        broadcastDeviceList();
        broadcastParticipantCount();

    });




    // ---- 매칭 초기화 (다음 라운드를 위해 전원 대기 상태로) ----

    socket.on('resetMatching', () => {

        if (!admins.has(socket.id)) {
            return;
        }


        devices.forEach((d) => {

            if (d.status === 'inactive') {
                return;
            }


            d.status = 'idle';
            d.resumeStatus = 'idle';

            d.character = null;
            d.partnerCharacter = null;

            d.inactiveRound = null;

        });


        io.emit('matchReset');

        broadcastDeviceList();
        broadcastParticipantCount();

    });




    // ---- 타이머 시작 (어드민 전용, 초 단위) ----

    socket.on('startTimer', (seconds) => {

        if (!admins.has(socket.id)) {
            return;
        }


        const dur = Number(seconds);


        if (!dur || dur <= 0) {
            return;
        }


        timerState = {

            running: true,

            endTime: Date.now() + dur * 1000,

            durationSec: dur,

        };


        io.emit('timerStatus', timerState);

    });



    socket.on('stopTimer', () => {

        if (!admins.has(socket.id)) {
            return;
        }


        timerState = {

            running: false,

            endTime: null,

            durationSec: null,

        };


        io.emit('timerStatus', timerState);

    });




    // ---- 연결 해제 ----

    socket.on('disconnect', () => {

        admins.delete(socket.id);


        const clientId = socket.data.clientId;

        const d = clientId
            ? devices.get(clientId)
            : null;



        if (d && d.socketId === socket.id) {

            d.resumeStatus =
                d.status === 'inactive'
                    ? d.resumeStatus || 'idle'
                    : d.status;


            d.status = 'inactive';

            d.inactiveRound = currentRound;

        }



        broadcastDeviceList();
        broadcastParticipantCount();

    });

});



const PORT = process.env.PORT || 3000;


server.listen(PORT, () => {

    console.log(
        '서버 실행 중: http://localhost:' + PORT
    );

});

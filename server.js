// 引入需要的模块
const http = require('http');
const url = require('url');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

function generateId() {
  return crypto.randomBytes(3).toString('hex');
}

const rooms = new Map();

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css',
  '.js': 'application/javascript',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.mp3': 'audio/mpeg',
  '.wav': 'audio/wav',
  '.ogg': 'audio/ogg',
  '.mp4': 'video/mp4',
  '.webm': 'video/webm',
};

const CACHE_EXT = new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.ico', '.mp3', '.wav', '.ogg', '.mp4', '.webm', '.css', '.js']);
const CACHE_MAX_AGE = 86400; // 1 day

const server = http.createServer((req, res) => {
  let filePath = path.join(__dirname, 'docs', req.url === '/' ? 'index.html' : req.url);
  const ext = path.extname(filePath).toLowerCase();
  const contentType = MIME[ext] || 'application/octet-stream';

  // Security: prevent directory traversal
  if (!filePath.startsWith(path.join(__dirname, 'docs'))) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.readFile(filePath, (err, content) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
    } else {
      const headers = { 'Content-Type': contentType };
      if (CACHE_EXT.has(ext)) {
        headers['Cache-Control'] = `public, max-age=${CACHE_MAX_AGE}`;
      }
      res.writeHead(200, headers);
      res.end(content);
    }
  });
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const parameters = url.parse(req.url, true).query;
  const roomId = parameters.room;
  const playerId = generateId();
  let room;

  if (!roomId || !rooms.has(roomId)) {
    const newRoomId = roomId || generateId();
    room = {
      id: newRoomId,
      players: [{ ws, id: playerId, ready: false, name: 'Player1' }],
      state: null,
    };
    rooms.set(newRoomId, room);
  } else {
    room = rooms.get(roomId);
    if (room.players.length >= 2) {
      ws.send(JSON.stringify({ type: 'ERROR', message: '房间已满' }));
      ws.close();
      return;
    }
    room.players.push({ ws, id: playerId, ready: false, name: 'Player2' });
  }

  ws.send(JSON.stringify({ type: 'ROOM_INFO', roomId: room.id, playerId }));

  if (room.players.length === 2) {
    room.players.forEach(p => p.ws.send(JSON.stringify({
      type: 'MATCH_FOUND',
      players: room.players.map(p => ({ id: p.id })),
    })));
  }

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      handleMessage(playerId, msg);
    } catch (e) {
      console.error('消息解析失败', e);
    }
  });

  ws.on('close', () => {
    if (room) {
      room.players = room.players.filter(p => p.ws !== ws);
      if (room.players.length === 0) {
        rooms.delete(room.id);
      } else {
        room.players[0].ws.send(JSON.stringify({ type: 'OPPONENT_DISCONNECTED' }));
      }
    }
  });
});

// ------------------ 游戏逻辑 ------------------
function createInitialState() {
  return {
    phase: 'preGame',
    turn: null,
    players: [],
    turnNumber: 0,
    winner: null,
    peaceActive: false,
    peaceRounds: 0,
  };
}

function initGameState(player1, player2, firstPlayerId) {
  const state = createInitialState();
  state.players = [makePlayer(player1), makePlayer(player2)];
  state.phase = 'rps';
  state.rpsChoices = {};
  state.phaseDeadline = Date.now() + 5000;
  return state;
}

function makePlayer(base) {
  return {
    id: base.id,
    hp: 3,
    leftHand: 0,
    rightHand: 0,
    shield: 0,
    shieldTurns: 0,
    usedNineRevive: false,
    sevenSevenActive: 0,
    cutHands: [],
    opPool: [],
    avatar: base.avatar || 'touxiang/touxiang1.jpg',
    nickname: base.nickname || '',
    stats: { damageDealt: 0, damageTaken: 0, hpHealed: 0, shieldGenerated: 0 },
  };
}

function randomStartHand() {
  return Math.floor(Math.random() * 3) + 1;
}

function broadcast(room) {
  const state = room.state;
  state._stateSeq = (state._stateSeq || 0) + 1;
  const publicState = JSON.parse(JSON.stringify(state));
  room.players.forEach(p => {
    if (p.ws.readyState === 1) {
      p.ws.send(JSON.stringify({ type: 'STATE_UPDATE', state: publicState }));
    }
  });
}

function handleMessage(playerId, msg) {
  // P2P 转发：房主替客机代发消息时携带 senderId
  const effectivePlayerId = msg.senderId || playerId;

  let targetRoom = null;
  for (const [_, room] of rooms) {
    if (room.players.some(p => p.id === effectivePlayerId)) {
      targetRoom = room;
      break;
    }
  }
  if (!targetRoom) return;

  const room = targetRoom;
  const player = room.players.find(p => p.id === effectivePlayerId);

  switch (msg.type) {
    case 'PROFILE':
      player.avatar = msg.avatar;
      player.nickname = msg.nickname;
      break;

    case 'READY':
      player.ready = true;
      if (msg.avatar) player.avatar = msg.avatar;
      if (msg.nickname) player.nickname = msg.nickname;
      if (room.players.length === 2 && room.players.every(p => p.ready)) {
        room.state = initGameState(room.players[0], room.players[1]);
        broadcast(room);
        startRpsTimer(room);
      } else {
        player.ws.send(JSON.stringify({ type: 'WAITING_OPPONENT' }));
      }
      break;

    case 'ACT':
      if (!room.state || room.state.phase !== 'playing') return;
      if (room.state.turn !== effectivePlayerId) {
        broadcastError(room, effectivePlayerId, '不是你的回合');
        sendResumeTurn(room, effectivePlayerId);
        return;
      }
      clearPhaseTimer(room);
      processAction(room, effectivePlayerId, msg);
      break;

    case 'SKILL_CHOICE':
      if (!room.state || room.state.phase !== 'playing') return;
      if (room.state.turn !== effectivePlayerId) return;
      processSkillChoice(room, effectivePlayerId, msg);
      break;

    case 'PAY_CUT':
      if (!room.state || room.state.phase !== 'playing') return;
      handlePayCut(room, effectivePlayerId, msg);
      break;

    case 'DEBUG_SET_HAND':
      if (!room.state || room.state.phase !== 'playing') return;
      handleDebugSetHand(room, effectivePlayerId, msg);
      break;

    case 'TRIGGER_EFFECT':
      room.players.forEach(p => {
        if (p.ws.readyState === 1) {
          p.ws.send(JSON.stringify({
            type: 'PLAY_TARGET_EFFECT',
            skillId: msg.skillId,
            targetHand: msg.targetHand,
            casterId: effectivePlayerId,
          }));
        }
      });
      break;

    case 'REMATCH':
      if (room.players.length < 2) {
        player.ws.send(JSON.stringify({ type: 'REMATCH_STATUS', opponentLeft: true }));
        break;
      }
      if (!room.rematchReady) room.rematchReady = new Set();
      room.rematchReady.add(effectivePlayerId);
      if (room.rematchReady.size >= 2) {
        room.rematchReady.clear();
        room.state = initGameState(room.players[0], room.players[1]);
        broadcast(room);
        startRpsTimer(room);
      } else {
        room.players.forEach(p => {
          if (p.ws.readyState === 1) {
            p.ws.send(JSON.stringify({
              type: 'REMATCH_STATUS',
              readyCount: room.rematchReady.size,
            }));
          }
        });
      }
      break;

    case 'RPS_CHOICE':
      if (!room.state || room.state.phase !== 'rps') return;
      if (room.state.rpsChoices[effectivePlayerId]) return;
      if (!['rock', 'scissors', 'paper'].includes(msg.choice)) return;
      room.state.rpsChoices[effectivePlayerId] = msg.choice;
      if (Object.keys(room.state.rpsChoices).length === 2) {
        resolveRPS(room);
      } else {
        broadcast(room);
      }
      break;

    case 'HAND_SELECT':
      if (!room.state || room.state.phase !== 'handSelect') return;
      if (room.state.handChoices[effectivePlayerId]) return;
      const lh = parseInt(msg.leftHand), rh = parseInt(msg.rightHand);
      if (![1,2,3].includes(lh) || ![1,2,3].includes(rh)) return;
      room.state.handChoices[effectivePlayerId] = { left: lh, right: rh };
      if (Object.keys(room.state.handChoices).length === 2) {
        resolveHandSelect(room);
      } else {
        broadcast(room);
      }
      break;

    case 'SIGNAL_OFFER':
    case 'SIGNAL_ANSWER':
    case 'SIGNAL_ICE':
      room.players.forEach(p => {
        if (p.id !== effectivePlayerId && p.ws.readyState === 1) {
          p.ws.send(JSON.stringify(msg));
        }
      });
      break;

    default:
      break;
  }
}

// 行动处理
function processAction(room, playerId, msg) {
  const { attackHand, targetHand, operation } = msg;
  const state = room.state;
  const attacker = state.players.find(p => p.id === playerId);
  const defender = state.players.find(p => p.id !== playerId);
  if (!attacker || !defender) return;

  if (isHandDisabled(attacker, attackHand)) {
    broadcastError(room, playerId, '该手无法攻击（被砍或封印）');
    sendResumeTurn(room, playerId);
    return;
  }
  if (!isHandTargetable(defender, targetHand)) {
    broadcastError(room, playerId, '目标手已被砍，无法选中');
    sendResumeTurn(room, playerId);
    return;
  }

  let a = attackHand === 'left' ? attacker.leftHand : attacker.rightHand;
  let b = targetHand === 'left' ? defender.leftHand : defender.rightHand;

  if (operation === 'sub' && !attacker.opPool.includes('sub')) {
    broadcastError(room, playerId, '没有减法机会');
    sendResumeTurn(room, playerId);
    return;
  }
  if (operation === 'mul' && !attacker.opPool.includes('mul')) {
    broadcastError(room, playerId, '没有乘法机会');
    sendResumeTurn(room, playerId);
    return;
  }
  if (operation === 'div' && !attacker.opPool.includes('div')) {
    broadcastError(room, playerId, '没有除法机会');
    sendResumeTurn(room, playerId);
    return;
  }

  let newValue;
  switch (operation) {
    case 'add': newValue = (a + b) % 10; break;
    case 'sub': newValue = Math.abs(a - b) % 10; break;
    case 'mul': newValue = (a * b) % 10; break;
    case 'div': {
      const divisor = b === 0 ? 10 : b;
      const result = a / divisor;
      const str = Math.abs(result).toString().replace('.', '');
      let firstNonZero = '0';
      for (let ch of str) {
        if (ch !== '0') { firstNonZero = ch; break; }
      }
      newValue = parseInt(firstNonZero, 10);
      break;
    }
    default: newValue = a;
  }

  if (operation === 'sub') attacker.opPool = attacker.opPool.filter(x => x !== 'sub');
  if (operation === 'mul') attacker.opPool = attacker.opPool.filter(x => x !== 'mul');
  if (operation === 'div') attacker.opPool = attacker.opPool.filter(x => x !== 'div');

  if (attackHand === 'left') attacker.leftHand = newValue;
  else attacker.rightHand = newValue;

  // 写入操作提示到状态（双方可见，客户端判断是否显示）
  const opMap = { add: '+', sub: '-', mul: '×', div: '÷' };
  const opSymbol = opMap[operation] || operation;
  state.lastActionNotice = {
    attackerId: playerId,
    text: `${attacker.nickname || '对手'} ${a}${opSymbol}${b}=${newValue}`,
    time: Date.now()
  };

  broadcast(room);

  state.lastAction = { attackerId: playerId, defenderId: defender.id, newValue, attackHand };

  const forcedCombo = checkForcedCombo(attacker);
  if (forcedCombo) {
    broadcastSkillCast(room, attacker, { id: forcedCombo, description: getComboDescription(forcedCombo) });
    executeCombo(room, forcedCombo, attacker, defender, true, 'left');
    afterAction(room);
    return;
  }

  const options = getSkillOptions(attacker, newValue, attackHand);
  if (options.length === 0) {
    afterAction(room);
    return;
  }

  // 仅剩单个纯正面技能 → 自动发动（不可跳过）
  if (options.length === 1 && isPurePositive(options[0].id)) {
    const auto = options[0];
    broadcastSkillCast(room, attacker, auto);
    if (auto.needsTarget) {
      // 仍需选目标手：客户端直接进入"点击对方手"模式
      attacker._pendingSkillOptions = [auto];
      room.players.find(p => p.id === playerId).ws.send(JSON.stringify({
        type: 'SKILL_AUTO_TARGET',
        skillId: auto.id,
        description: auto.description,
      }));
    } else {
      executeCombo(room, auto.id, attacker, defender, false, 'left');
      afterAction(room);
    }
    return;
  }

  // 多选或唯一带代价（combo_910）→ 弹出选择面板
  attacker._pendingSkillOptions = options;
  room.players.find(p => p.id === playerId).ws.send(JSON.stringify({
    type: 'SKILL_CHOICE_REQUEST',
    options,
  }));
}

function processSkillChoice(room, playerId, msg) {
  const { skillId, targetHand } = msg;
  const state = room.state;
  const attacker = state.players.find(p => p.id === playerId);
  const defender = state.players.find(p => p.id !== playerId);
  if (!attacker || !attacker._pendingSkillOptions) return;

  const wasRecoveryTrigger = !!attacker._recoveryTrigger;
  delete attacker._recoveryTrigger;

  if (skillId === 'none' || !skillId) {
    delete attacker._pendingSkillOptions;
    if (wasRecoveryTrigger) {
      broadcast(room);
    } else {
      afterAction(room);
    }
    return;
  }

  const option = attacker._pendingSkillOptions.find(opt => opt.id === skillId);
  if (!option) return;

  if (option.needsTarget && !targetHand) {
    msg.targetHand = 'left';
  }

  delete attacker._pendingSkillOptions;
  broadcastSkillCast(room, attacker, option, targetHand ? { targetHand } : undefined);
  executeCombo(room, skillId, attacker, defender, false, targetHand);

  if (wasRecoveryTrigger) {
    if (checkDeath(room)) return;
    broadcast(room);
  } else {
    afterAction(room);
  }
}

function handlePayCut(room, playerId, msg) {
  const { hand } = msg;
  const player = room.state.players.find(p => p.id === playerId);
  if (!player) return;

  const cutIndex = player.cutHands.findIndex(c => c.hand === hand && c.type === 'cut' && c.canPay);
  if (cutIndex === -1) {
    broadcastError(room, playerId, '不能支付恢复此手');
    return;
  }

  player.hp -= 1;
  if (hand === 'left') player.leftHand = 1;
  else player.rightHand = 1;
  player.cutHands.splice(cutIndex, 1);

  if (checkDeath(room)) return;

  // 恢复成 1 后，按"正常运算后"流程检测可触发的强制连招或可选技能
  tryRecoveryTrigger(room, player, hand);
  if (checkDeath(room)) return;

  broadcast(room);
}

function handleDebugSetHand(room, playerId, msg) {
  const { hand, value } = msg;
  const state = room.state;
  const player = state.players.find(p => p.id === playerId);
  const opponent = state.players.find(p => p.id !== playerId);
  if (!player || !opponent) return;

  if (isHandDisabled(player, hand)) {
    broadcastError(room, playerId, '该手被砍或封印，无法调试');
    return;
  }
  if (typeof value !== 'number' || value < 0 || value > 9) {
    broadcastError(room, playerId, '数值必须在 0-9 之间');
    return;
  }

  if (hand === 'left') player.leftHand = value;
  else player.rightHand = value;

  state.lastAction = { attackerId: playerId, defenderId: opponent.id, newValue: value, attackHand: hand };

  broadcast(room);

  const forcedCombo = checkForcedCombo(player);
  if (forcedCombo) {
    broadcastSkillCast(room, player, { id: forcedCombo, description: getComboDescription(forcedCombo) });
    executeCombo(room, forcedCombo, player, opponent, true, 'left');
    afterAction(room);
    return;
  }

  const options = getSkillOptions(player, value, hand);
  if (options.length === 0) {
    afterAction(room);
    return;
  }

  if (options.length === 1 && isPurePositive(options[0].id)) {
    const auto = options[0];
    broadcastSkillCast(room, player, auto);
    if (auto.needsTarget) {
      player._pendingSkillOptions = [auto];
      room.players.find(p => p.id === playerId).ws.send(JSON.stringify({
        type: 'SKILL_AUTO_TARGET',
        skillId: auto.id,
        description: auto.description,
      }));
    } else {
      executeCombo(room, auto.id, player, opponent, false, 'left');
      afterAction(room);
    }
    return;
  }

  player._pendingSkillOptions = options;
  room.players.find(p => p.id === playerId).ws.send(JSON.stringify({
    type: 'SKILL_CHOICE_REQUEST',
    options,
  }));
}

function afterAction(room) {
  const state = room.state;
  if (!state) return;

  if (checkDeath(room)) return;

  const attackerId = state.lastAction.attackerId;
  const attacker = state.players.find(p => p.id === attackerId);
  const defender = state.players.find(p => p.id !== attackerId);

  if (state._extraTurn && state._extraTurn.playerId === attackerId) {
    delete state._extraTurn;
    startTurn(room, attackerId);
    if (checkDeath(room)) return;
    skipStuckTurns(room);
    broadcast(room);
    return;
  }

  state.turn = state.turn === attackerId ? defender.id : attackerId;
  state.turnNumber++;
  delete state._extraTurnUsed;
  startTurn(room, state.turn);
  if (checkDeath(room)) return;
  skipStuckTurns(room);
  broadcast(room);
}

function startRpsTimer(room) {
  const state = room.state;
  state.phaseDeadline = Date.now() + 5000;
  setPhaseTimer(room, 5200, () => {
    if (!room.state || room.state.phase !== 'rps') return;
    const choices = ['rock', 'scissors', 'paper'];
    room.state.players.forEach(p => {
      if (!room.state.rpsChoices[p.id]) room.state.rpsChoices[p.id] = choices[Math.floor(Math.random() * 3)];
    });
    resolveRPS(room);
  });
}

// 石头剪刀布 结算
function resolveRPS(room) {
  const state = room.state;
  const [p1, p2] = state.players;
  const c1 = state.rpsChoices[p1.id];
  const c2 = state.rpsChoices[p2.id];

  if (c1 === c2) {
    // 平局，重新来
    state._lastRpsChoices = { ...state.rpsChoices };
    state.rpsChoices = {};
    state.rpsTie = true;
    state.phaseDeadline = Date.now() + 5000;
    broadcast(room);
    state.rpsTie = false;
    startRpsTimer(room);
    return;
  }

  const winMap = { rock: 'scissors', scissors: 'paper', paper: 'rock' };
  const winner = winMap[c1] === c2 ? p1 : p2;
  state.rpsResult = { winnerId: winner.id, choices: { [p1.id]: c1, [p2.id]: c2 } };
  state.phase = 'handSelect';
  state.handChoices = {};
  state.phaseDeadline = Date.now() + 20000;
  broadcast(room);
  startHandSelectTimer(room);
}

function startHandSelectTimer(room) {
  const state = room.state;
  state.phaseDeadline = Date.now() + 20000;
  setPhaseTimer(room, 20200, () => {
    if (!room.state || room.state.phase !== 'handSelect') return;
    room.state.players.forEach(p => {
      if (!room.state.handChoices[p.id]) {
        room.state.handChoices[p.id] = {
          left: Math.floor(Math.random() * 3) + 1,
          right: Math.floor(Math.random() * 3) + 1
        };
      }
    });
    resolveHandSelect(room);
  });
}

// 开局选手势 结算
function resolveHandSelect(room) {
  const state = room.state;
  const [p1, p2] = state.players;
  p1.leftHand = state.handChoices[p1.id].left;
  p1.rightHand = state.handChoices[p1.id].right;
  p2.leftHand = state.handChoices[p2.id].left;
  p2.rightHand = state.handChoices[p2.id].right;

  state.turn = state.rpsResult.winnerId;
  state.phase = 'playing';
  state.phaseDeadline = Date.now() + 60000;
  state._newGame = true;
  clearPhaseTimer(room);
  broadcast(room);
  state._newGame = false;
  startTurn(room, state.turn);
}

// 当前回合玩家两手皆被砍/封印时，自动跳过其回合（状态衰减照常执行）。
// skipCount 上限防止双方同时卡死引发的无限递归。
function skipStuckTurns(room) {
  const state = room.state;
  if (!state || state.phase !== 'playing') return;
  let skipCount = 0;
  while (skipCount < 2) {
    const cur = state.players.find(p => p.id === state.turn);
    if (!cur) break;
    if (!(isHandDisabled(cur, 'left') && isHandDisabled(cur, 'right'))) break;
    broadcastNotice(room, `${cur.nickname || '玩家'} 双手都无法行动，自动跳过该回合`);
    const other = state.players.find(p => p.id !== cur.id);
    state.turn = other.id;
    state.turnNumber++;
    startTurn(room, state.turn);
    if (checkDeath(room)) return;
    skipCount++;
  }
}

function broadcastNotice(room, message) {
  room.players.forEach(p => {
    if (p.ws.readyState === 1) {
      p.ws.send(JSON.stringify({ type: 'NOTICE', message }));
    }
  });
}

// 收敛死亡判定：循环处理可能同时倒下的玩家（如 combo_19 互伤双死），有 9 复活，否则结束游戏。
// 返回 true 表示已进入 gameOver（调用方应直接 return）。
function checkDeath(room) {
  const state = room.state;
  if (!state) return false;
  while (true) {
    const loser = state.players.find(p => p.hp <= 0);
    if (!loser) return false;
    if (hasNine(loser) && !loser.usedNineRevive) {
      loser.hp = 1;
      loser.usedNineRevive = true;
      broadcastSkillCast(room, loser, { id: 'nine_revive', description: '喝酒复活' });
      continue;
    }
    state.phase = 'gameOver';
    state.winner = state.players.find(p => p.id !== loser.id).id;
    clearPhaseTimer(room);
    broadcast(room);
    return true;
  }
}

// 被砍手恢复成 1 时，触发与正常运算后等效的技能检测。
// newValue=1 可触发：combo_19（强制）、combo_15（需选目标）、combo_61（纯正面）。
// 这些都是单一选项，不会出现需要弹大面板二选一的情况。
function tryRecoveryTrigger(room, player, recoveredHand) {
  const state = room.state;
  if (!state || state.phase !== 'playing') return;
  const opponent = state.players.find(p => p.id !== player.id);
  if (!opponent) return;

  const forced = checkForcedCombo(player);
  if (forced) {
    broadcastSkillCast(room, player, { id: forced, description: getComboDescription(forced) });
    executeCombo(room, forced, player, opponent, true, 'left');
    return;
  }

  const options = getSkillOptions(player, 1, recoveredHand);
  if (options.length === 0) return;

  if (options.length === 1 && isPurePositive(options[0].id)) {
    const auto = options[0];
    broadcastSkillCast(room, player, auto);
    if (auto.needsTarget) {
      // 需要选目标手；标记为恢复触发，processSkillChoice 不能切回合
      player._pendingSkillOptions = [auto];
      player._recoveryTrigger = true;
      const playerConn = room.players.find(p => p.id === player.id);
      if (playerConn && playerConn.ws.readyState === 1) {
        playerConn.ws.send(JSON.stringify({
          type: 'SKILL_AUTO_TARGET',
          skillId: auto.id,
          description: auto.description,
        }));
      }
    } else {
      executeCombo(room, auto.id, player, opponent, false, 'left');
    }
  }
}

// 阶段计时器管理
function clearPhaseTimer(room) {
  if (room._phaseTimer) { clearTimeout(room._phaseTimer); room._phaseTimer = null; }
}
function setPhaseTimer(room, ms, onTimeout) {
  clearPhaseTimer(room);
  room._phaseTimer = setTimeout(() => {
    room._phaseTimer = null;
    onTimeout();
  }, ms);
}

function startTurn(room, playerId) {
  const state = room.state;
  const player = state.players.find(p => p.id === playerId);
  if (!player) return;

  if (state.peaceActive) {
    state.peaceRounds--;
    if (state.peaceRounds <= 0) {
      state.peaceActive = false;
    }
  }

  if (player.shieldTurns > 0) {
    player.shieldTurns--;
    if (player.shieldTurns === 0) player.shield = 0;
  }

  if (player.sevenSevenActive > 0) {
    player.sevenSevenActive--;
  }

  let recoveredHand = null;
  player.cutHands = player.cutHands.map(cut => {
    const newCut = { ...cut };
    newCut.turnsLeft--;
    if (newCut.type === 'cut') {
      newCut.canPay = (newCut.turnsLeft === 2);
    }
    if (newCut.turnsLeft <= 0) {
      if (newCut.type === 'cut') {
        if (newCut.hand === 'left') player.leftHand = 1;
        else player.rightHand = 1;
        recoveredHand = newCut.hand;
      }
      return null;
    }
    return newCut;
  }).filter(cut => cut !== null);

  if (recoveredHand) {
    tryRecoveryTrigger(room, player, recoveredHand);
  }

  // 45s回合倒计时
  state.phaseDeadline = Date.now() + 60000;
  setPhaseTimer(room, 60200, () => {
    if (room.state && room.state.phase === 'playing' && room.state.turn === playerId) {
      broadcastNotice(room, `${player.nickname || '玩家'} 超时，系统随机行动`);
      randomAct(room, playerId);
    }
  });
}

// 超时随机合法行动
function randomAct(room, playerId) {
  const state = room.state;
  const attacker = state.players.find(p => p.id === playerId);
  const defender = state.players.find(p => p.id !== playerId);
  if (!attacker || !defender) return;

  const hands = ['left', 'right'];
  const validAttack = hands.filter(h => !isHandDisabled(attacker, h));
  const validTarget = hands.filter(h => !isHandTargetable(defender, h));
  if (validAttack.length === 0 || validTarget.length === 0) {
    // 无可选手，跳过回合
    broadcastNotice(room, `${attacker.nickname || '玩家'} 无合法操作，跳过回合`);
    const other = state.players.find(p => p.id !== playerId);
    state.turn = other.id;
    state.turnNumber++;
    startTurn(room, state.turn);
    skipStuckTurns(room);
    broadcast(room);
    return;
  }

  const ops = ['add'];
  if (attacker.opPool.includes('sub')) ops.push('sub');
  if (attacker.opPool.includes('mul')) ops.push('mul');
  if (attacker.opPool.includes('div')) ops.push('div');

  const attackHand = validAttack[Math.floor(Math.random() * validAttack.length)];
  const targetHand = validTarget[Math.floor(Math.random() * validTarget.length)];
  const operation = ops[Math.floor(Math.random() * ops.length)];

  processAction(room, playerId, { attackHand, targetHand, operation });
}

function isHandDisabled(player, hand) {
  return player.cutHands.some(c => c.hand === hand);
}

function isHandOccupied(player, hand) {
  return player.cutHands.some(c => c.hand === hand);
}

function isHandTargetable(player, hand) {
  return !player.cutHands.some(c => c.hand === hand && c.type === 'cut');
}

function getComboByHands(player) {
  let l = player.leftHand, r = player.rightHand;
  const leftCut = player.cutHands.some(c => c.hand === 'left' && c.type === 'cut');
  const rightCut = player.cutHands.some(c => c.hand === 'right' && c.type === 'cut');

  if (leftCut) l = null;
  if (rightCut) r = null;

  if (l === null && r === null) return null;
  if (l === null) {
    l = r;
    r = null;
  }
  if (r === null) return null;

  const pair = [l, r].sort((a, b) => a - b).join(',');
  const map = {
    '2,2': 'combo_22',
    '1,9': 'combo_19',
    '6,9': 'combo_69',
    '1,6': 'combo_61',
    '2,6': 'combo_62',
    '3,6': 'combo_63',
    '0,9': 'combo_910',
    '1,5': 'combo_15',
    '5,8': 'combo_58',
    '0,5': 'combo_510',
    '5,5': 'combo_55',
    '0,0': 'combo_1010',
    '7,7': 'combo_77',
    '8,8': 'combo_88',
  };
  return map[pair] || null;
}

function checkForcedCombo(player) {
  const l = player.leftHand;
  const r = player.rightHand;
  if ((l === 1 && r === 9) || (l === 9 && r === 1)) return 'combo_19';
  if ((l === 6 && r === 9) || (l === 9 && r === 6)) return 'combo_69';
  if ((l === 5 && r === 8) || (l === 8 && r === 5)) return 'combo_58';
  return null;
}

function getSkillOptions(player, newValue, changedHand) {
  let options = [];
  const combo = getComboByHands(player);

  if ([4, 5, 6, 8, 0].includes(newValue)) {
    const opt = { id: `single_${newValue}`, description: getSingleDescription(newValue) };
    if (newValue === 4) opt.needsTarget = true;
    options.push(opt);
  }

  if (combo) {
    const opt = { id: combo, description: getComboDescription(combo) };
    if (combo === 'combo_15') opt.needsTarget = true;
    options.push(opt);
  }

  // 组合技效果完全覆盖对应单手效果时，舍去单手选项
  const COMBO_COVERS_SINGLE = {
    'combo_15': ['single_5'],      // 封印+2盾 覆盖 护盾
    'combo_55': ['single_5'],      // 降龙十八掌+2盾 覆盖 护盾
    'combo_510': ['single_5', 'single_0'], // 恭敬拳+2盾 覆盖 护盾+打拳
    'combo_88': ['single_8'],      // 双枪打击(2伤) 覆盖 打手枪(1伤)
    'combo_1010': ['single_0'],    // 降龙十八拳(2真伤) 覆盖 打拳(1伤)
  };
  for (const [comboId, singleIds] of Object.entries(COMBO_COVERS_SINGLE)) {
    if (options.some(o => o.id === comboId)) {
      options = options.filter(o => !singleIds.includes(o.id));
    }
  }
  return options;
}

// 带自我代价的技能集合（非纯正面）；其余主动技能默认纯正面
const SKILL_HAS_SELF_COST = new Set(['combo_910']);
function isPurePositive(skillId) {
  return !SKILL_HAS_SELF_COST.has(skillId);
}

function broadcastSkillCast(room, attacker, option, extra) {
  room.players.forEach(p => {
    if (p.ws.readyState === 1) {
      const msg = {
        type: 'SKILL_CAST',
        skillId: option.id,
        description: option.description,
        casterId: attacker.id,
      };
      if (extra) Object.assign(msg, extra);
      p.ws.send(JSON.stringify(msg));
    }
  });
}

function getSingleDescription(val) {
  const map = {
    4: '挥砍',
    5: '护盾',
    6: '饮甘露',
    8: '打手枪',
    0: '打拳',
  };
  return map[val] || '';
}

function getComboDescription(combo) {
  const map = {
    combo_22: '加智力',
    combo_19: '海角之乐',
    combo_69: '云雨之乐',
    combo_61: '射箭',
    combo_62: '二重连矢',
    combo_63: '三连破矢',
    combo_910: '自爆手雷',
    combo_15: '封印',
    combo_58: '五八同城',
    combo_510: '恭敬拳',
    combo_55: '降龙十八掌',
    combo_1010: '白金连打',
    combo_77: '仙人模式',
    combo_88: '双枪打击',
  };
  return map[combo] || '';
}

function executeCombo(room, comboId, attacker, defender, isForced, targetHand = 'left') {
  const state = room.state;

  const safeDamage = (target, amount, type) => {
    if (state.peaceActive) return;
    if (target === defender) {
      attacker.stats.damageDealt += amount;
    }
    target.stats.damageTaken += amount;
    dealDamage(target, amount, type);
  };

  const addSageDamage = () => {
    if (attacker.sevenSevenActive > 0 && !state.peaceActive) {
      attacker.stats.damageDealt += 1;
      defender.stats.damageTaken += 1;
      dealDamage(defender, 1, 'true');
    }
  };

  switch (comboId) {
    case 'combo_22': {
      const ops = ['sub', 'mul', 'div'];
      const gained = ops[Math.floor(Math.random() * ops.length)];
      attacker.opPool.push(gained);
      break;
    }
    case 'combo_19': {
      safeDamage(attacker, 1, 'normal');
      safeDamage(defender, 1, 'normal');
      addSageDamage();
      if (attacker.hp < defender.hp && !state._extraTurnUsed) {
        state._extraTurn = { playerId: attacker.id };
        state._extraTurnUsed = true;
        state.extraTurnNotice = { playerId: attacker.id, text: '🔥 连动！获得额外回合', time: Date.now() };
      }
      break;
    }
    case 'combo_69': {
      heal(attacker, 1);
      heal(defender, 1);
      if (attacker.hp > defender.hp && !state._extraTurnUsed) {
        state._extraTurn = { playerId: attacker.id };
        state._extraTurnUsed = true;
        state.extraTurnNotice = { playerId: attacker.id, text: '🔥 连动！获得额外回合', time: Date.now() };
      }
      break;
    }
    case 'combo_61': {
      safeDamage(defender, 1, 'normal');
      addSageDamage();
      break;
    }
    case 'combo_62': {
      if (defender.shield === 1) {
        defender.shield = 0;
        defender.shieldTurns = 0;
        safeDamage(defender, 1, 'normal');
      } else if (defender.shield === 2) {
        defender.shield = 0;
        defender.shieldTurns = 0;
      } else {
        safeDamage(defender, 1, 'normal');
      }
      addSageDamage();
      break;
    }
    case 'combo_63': {
      defender.shield = 0;
      defender.shieldTurns = 0;
      safeDamage(defender, 1, 'normal');
      addSageDamage();
      break;
    }
    case 'combo_910': {
      safeDamage(attacker, 2, 'normal');
      safeDamage(defender, 4, 'normal');
      addSageDamage();
      break;
    }
    case 'combo_15': {
      if (isHandOccupied(defender, targetHand)) {
        broadcastError(room, attacker.id, '该手已被砍或封印，无法重复选择');
        sendResumeTurn(room, attacker.id);
        return;
      }
      attacker.shield = 2;
      attacker.shieldTurns = 2;
      attacker.stats.shieldGenerated += 2;
      defender.cutHands.push({ hand: targetHand, turnsLeft: 2, canPay: false, type: 'lock' });
      break;
    }
    case 'combo_58': {
      state.peaceActive = true;
      state.peaceRounds = 6;
      break;
    }
    case 'combo_510': {
      attacker.shield = 2;
      attacker.shieldTurns = 2;
      attacker.stats.shieldGenerated += 2;
      safeDamage(defender, 1, 'true');
      addSageDamage();
      break;
    }
    case 'combo_55': {
      attacker.shield = 2;
      attacker.shieldTurns = 2;
      attacker.stats.shieldGenerated += 2;
      safeDamage(defender, 1, 'normal');
      addSageDamage();
      break;
    }
    case 'combo_1010': {
      safeDamage(defender, 2, 'true');
      addSageDamage();
      break;
    }
    case 'combo_77': {
      heal(attacker, 2);
      attacker.sevenSevenActive = 3;
      break;
    }
    case 'combo_88': {
      safeDamage(defender, 2, 'normal');
      addSageDamage();
      break;
    }
    case 'single_4': {
      if (isHandOccupied(defender, targetHand)) {
        broadcastError(room, attacker.id, '该手已被砍或封印，无法重复选择');
        sendResumeTurn(room, attacker.id);
        return;
      }
      defender.cutHands.push({ hand: targetHand, turnsLeft: 3, canPay: false, type: 'cut' });
      const cutHandsNow = defender.cutHands.filter(c => c.type === 'cut');
      if (cutHandsNow.length === 2) {
        const firstCut = defender.cutHands.find(c => c.type === 'cut');
        if (firstCut) {
          if (firstCut.hand === 'left') defender.leftHand = 1;
          else defender.rightHand = 1;
          defender.cutHands = defender.cutHands.filter(c => c !== firstCut);
          defender.hp -= 1;
          if (defender.hp <= 0) {
            if (hasNine(defender) && !defender.usedNineRevive) {
              defender.hp = 1;
              defender.usedNineRevive = true;
              broadcastSkillCast(room, defender, { id: 'nine_revive', description: '喝酒复活' });
            }
          }
        }
      }
      break;
    }
    case 'single_5': {
      attacker.shield = 2;
      attacker.shieldTurns = 2;
      attacker.stats.shieldGenerated += 2;
      break;
    }
    case 'single_6': {
      heal(attacker, 1);
      break;
    }
    case 'single_8': {
      safeDamage(defender, 1, 'normal');
      addSageDamage();
      break;
    }
    case 'single_0': {
      safeDamage(defender, 1, 'normal');
      addSageDamage();
      break;
    }
  }
}

function dealDamage(target, amount, type) {
  if (type === 'normal') {
    if (target.shield > 0) {
      if (amount <= target.shield) {
        target.shield -= amount;
        if (target.shield === 0) target.shieldTurns = 0;
        return;
      } else {
        amount -= target.shield;
        target.shield = 0;
        target.shieldTurns = 0;
      }
    }
  }
  target.hp -= amount;
  if (target.hp < 0) target.hp = 0;
}

function heal(player, amount) {
  player.hp = Math.min(player.hp + amount, 10);
  player.stats.hpHealed += amount;
}

function hasNine(player) {
  return player.leftHand === 9 || player.rightHand === 9;
}

function broadcastError(room, playerId, message) {
  const player = room.players.find(p => p.id === playerId);
  if (player) player.ws.send(JSON.stringify({ type: 'ERROR', message }));
}

function sendResumeTurn(room, playerId) {
  const player = room.players.find(p => p.id === playerId);
  if (player && player.ws.readyState === 1) {
    player.ws.send(JSON.stringify({ type: 'RESUME_TURN' }));
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('服务器已启动，端口：' + PORT);
});
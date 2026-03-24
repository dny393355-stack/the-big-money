const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');
const { RSI, EMA } = require('technicalindicators');
const { DateTime } = require('luxon');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

let candles = [];
let tradeHistory = []; // היסטוריית עסקאות
let lastSignal = null;
let lastSignalPrice = 0;

const wsKlines = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@kline_1m');
wsKlines.on('message', (data) => {
    const message = JSON.parse(data);
    const k = message.k;
    if (k.x) {
        candles.push({ high: parseFloat(k.h), low: parseFloat(k.l), close: parseFloat(k.c), time: k.t });
        if (candles.length > 50) candles.shift();
    }
});

const wsTicker = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@ticker');
wsTicker.on('message', (data) => {
    const message = JSON.parse(data);
    const price = parseFloat(message.c);
    
    const now = DateTime.now().setZone('UTC');
    const msToNext5Min = (5 - (now.minute % 5)) * 60000 - (now.second * 1000) - now.millisecond;
    const secondsLeft = Math.floor(msToNext5Min / 1000);

    // זיהוי סוף סבב (כשהטיימר מתאפס) לשמירת היסטוריה
    if (secondsLeft === 299 && lastSignal) {
        const result = price > lastSignalPrice ? 'YES' : 'NO';
        tradeHistory.unshift({
            time: DateTime.now().toFormat('HH:mm'),
            prediction: lastSignal,
            result: result,
            win: lastSignal === result
        });
        if (tradeHistory.length > 5) tradeHistory.pop();
        lastSignal = null;
    }

    const signal = analyzeMarket(price);
    
    // אם נשארו 30 שניות לסיום, אנחנו "נועלים" את הסיגנל האחרון לבדיקה
    if (secondsLeft === 30) {
        lastSignal = signal.verdict;
        lastSignalPrice = price;
    }

    io.emit('marketUpdate', { 
        price, 
        secondsLeft, 
        ...signal,
        history: tradeHistory,
        vol24h: parseFloat(message.q)
    });
});

function analyzeMarket(price) {
    if (candles.length < 20) return { rsi: '--', trend: '--', fvg: 'טוען...', finalScore: 0, verdict: 'WAIT' };
    
    const closes = candles.map(c => c.close);
    const rsi = RSI.calculate({ values: closes, period: 14 });
    const lastRSI = rsi[rsi.length - 1] || 50;
    
    let fvg = 'NONE';
    const c1 = candles[candles.length - 3], c3 = candles[candles.length - 1];
    if (c1 && c3) {
        if (c1.low > c3.high + 2) fvg = 'BEARISH';
        if (c1.high < c3.low - 2) fvg = 'BULLISH';
    }

    let score = 0;
    if (lastRSI < 30) score += 4; if (lastRSI > 70) score -= 4;
    if (fvg === 'BULLISH') score += 5; if (fvg === 'BEARISH') score -= 5;

    let verdict = 'NEUTRAL';
    if (score >= 4) verdict = 'YES';
    if (score <= -4) verdict = 'NO';

    return { rsi: lastRSI.toFixed(1), trend: score > 0 ? 'BULLISH' : 'BEARISH', fvg, finalScore: score, verdict };
}

server.listen(3000, () => console.log('🚀 המערכת מוכנה בפורט 3000'));
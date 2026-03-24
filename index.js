const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');
const { RSI, VWAP, BollingerBands } = require('technicalindicators');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// וודא שקובץ ה-index.html שלך נמצא בתיקיית public
app.use(express.static('public'));

let lastClosePrice = 0;
let prices = [];
let candles = [];
let history = []; 

const binanceWS = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@kline_5m');

binanceWS.on('message', (data) => {
    const msg = JSON.parse(data);
    const k = msg.k;
    const price = parseFloat(k.c);
    const isFinal = k.x;

    const now = Date.now();
    const msIn5Min = 5 * 60 * 1000;
    const nextBoundary = Math.ceil(now / msIn5Min) * msIn5Min;
    const secondsLeft = Math.floor((nextBoundary - now) / 1000);

    // --- עדכון קריטי: איפוס מחיר יעד בכל תחילת סבב ---
    if (!lastClosePrice || secondsLeft >= 299) {
        lastClosePrice = parseFloat(k.o);
    }

    prices.push(price);
    if (prices.length > 200) prices.shift();

    const rsiVal = calculateRSI(prices);
    const vwapVal = calculateVWAP(k);
    const bb = calculateBollinger(prices);
    const divergence = checkRSIDivergence(price, rsiVal);
    const fvg = detectFVG(k);

    const marketData = {
        price: price,
        lastClose: lastClosePrice,
        secondsLeft: secondsLeft,
        rsi: rsiVal,
        fvg: fvg,
        trend: price > vwapVal ? 'BULLISH' : 'BEARISH',
        divergence: divergence,
        liquiditySweep: price < parseFloat(k.l) * 1.0002,
        candle: { o: k.o, h: k.h, l: k.l, c: k.c },
        history: history 
    };

    marketData.finalScore = calculateFinalScore(marketData, price, vwapVal, bb);
    io.emit('marketUpdate', marketData);

    if (isFinal) {
        const win = (price > lastClosePrice); 
        const predictionWasYes = (marketData.finalScore > 50); 
        
        history.unshift({
            time: new Date().toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit' }),
            prediction: predictionWasYes ? 'YES' : 'NO',
            result: win ? 'YES' : 'NO',
            success: (predictionWasYes === win)
        });

        if (history.length > 10) history.pop(); 

        // עדכון מחיר יעד לסבב הבא ברגע שהנר נסגר
        lastClosePrice = price; 

        candles.push({ h: parseFloat(k.h), l: parseFloat(k.l), c: price });
        if (candles.length > 20) candles.shift();
    }
});

function calculateFinalScore(data, price, vwap, bb) {
    const diff = price - data.lastClose;
    const timeLeft = data.secondsLeft;
    const maxPossibleMove = timeLeft * 10; 

    if (timeLeft < 20 && Math.abs(diff) > maxPossibleMove) {
        return diff > 0 ? 99 : 1;
    }

    let score = 50;
    const timeWeight = (300 - timeLeft) / 300; 

    let techScore = 0;
    if (data.fvg === 'BULLISH') techScore += 10;
    if (data.fvg === 'BEARISH') techScore -= 10;
    if (data.rsi < 35) techScore += 15;
    if (data.rsi > 65) techScore -= 15;
    if (data.trend === 'BULLISH') techScore += 10; else techScore -= 10;

    const distanceScore = diff > 0 ? 45 : -45;
    score = 50 + (techScore * (1 - timeWeight)) + (distanceScore * timeWeight);

    return Math.min(99, Math.max(1, Math.round(score)));
}

function calculateRSI(values) {
    if (values.length < 14) return 50;
    const result = RSI.calculate({ values: values.slice(-15), period: 14 });
    return result[result.length - 1] || 50;
}

function calculateVWAP(k) {
    const vwap = VWAP.calculate({
        high: [parseFloat(k.h)], low: [parseFloat(k.l)],
        close: [parseFloat(k.c)], volume: [parseFloat(k.v)]
    });
    return vwap[0] || parseFloat(k.c);
}

function calculateBollinger(values) {
    if (values.length < 20) return { upper: 0, lower: 0 };
    const result = BollingerBands.calculate({ period: 20, values: values.slice(-21), stdDev: 2 });
    return result[result.length - 1] || { upper: 0, lower: 0 };
}

function detectFVG(k) {
    if (candles.length < 2) return 'NONE';
    const prev = candles[candles.length - 2];
    if (prev.h < parseFloat(k.l)) return 'BULLISH';
    if (prev.l > parseFloat(k.h)) return 'BEARISH';
    return 'NONE';
}

function checkRSIDivergence(price, rsi) {
    if (prices.length < 2) return 'None';
    const prevPrice = prices[prices.length - 2];
    if (price > prevPrice && rsi < 45) return '🐻 Bearish';
    if (price < prevPrice && rsi > 55) return '🐮 Bullish';
    return 'None';
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 BEAST ENGINE ACTIVE ON PORT ${PORT}`);
});
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');
const axios = require('axios');
const { RSI, VWAP, BollingerBands } = require('technicalindicators');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

let lastClosePrice = 0;
let prices = [];
let candles = [];
let history = []; 
let lockedDecision = null; // משתנה חדש לנעילת ההחלטה ב-2.5 דקות

async function getPythPrice() {
    try {
        const pythPriceId = "0xe62df6c8b4a8599688d2590a39ec7847776324831ff3f73f35558f07aa9ca2bc";
        const response = await axios.get(`https://hermes.pyth.network/v2/updates/price/latest?ids[]=${pythPriceId}`);
        const priceData = response.data.parsed[0].price;
        return parseFloat(priceData.price) * Math.pow(10, priceData.expo);
    } catch (error) {
        return null;
    }
}

const binanceWS = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@kline_5m');

binanceWS.on('message', async (data) => {
    const msg = JSON.parse(data);
    const k = msg.k;
    const price = parseFloat(k.c);
    const isFinal = k.x;

    const now = Date.now();
    const msIn5Min = 5 * 60 * 1000;
    const nextBoundary = Math.ceil(now / msIn5Min) * msIn5Min;
    const secondsLeft = Math.floor((nextBoundary - now) / 1000);

    const indexPrice = await getPythPrice();

    if (!lastClosePrice || secondsLeft >= 299) {
        lastClosePrice = indexPrice || parseFloat(k.o);
        lockedDecision = null; // איפוס הנעילה בסבב חדש
    }

    prices.push(price);
    if (prices.length > 200) prices.shift();

    const rsiVal = calculateRSI(prices);
    const vwapVal = calculateVWAP(k);
    const bb = calculateBollinger(prices);
    const divergence = checkRSIDivergence(price, rsiVal);
    const fvg = detectFVG(k);

    const currentPriceForScore = indexPrice || price;

    // --- לוגיקת נעילת החלטה ב-2.5 דקות (150 שניות לסיום) ---
    if (secondsLeft <= 150 && !lockedDecision) {
        const scoreAtLock = calculateFinalScore({lastClose: lastClosePrice, secondsLeft: secondsLeft, fvg: fvg, rsi: rsiVal, trend: price > vwapVal ? 'BULLISH' : 'BEARISH'}, currentPriceForScore, vwapVal, bb);
        
        lockedDecision = {
            score: scoreAtLock,
            action: scoreAtLock >= 75 ? 'STRONG YES' : scoreAtLock <= 25 ? 'STRONG NO' : 'NEUTRAL',
            time: new Date().toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
            reason: `RSI: ${rsiVal.toFixed(1)} | FVG: ${fvg} | DIST: ${(currentPriceForScore - lastClosePrice).toFixed(2)}`
        };
    }

    const marketData = {
        price: price,
        indexPrice: indexPrice,
        lastClose: lastClosePrice,
        secondsLeft: secondsLeft,
        rsi: rsiVal,
        fvg: fvg,
        trend: price > vwapVal ? 'BULLISH' : 'BEARISH',
        divergence: divergence,
        liquiditySweep: price < parseFloat(k.l) * 1.0002,
        candle: { o: k.o, h: k.h, l: k.l, c: k.c },
        history: history,
        lockedDecision: lockedDecision, // שליחת ההחלטה הנעולה לאתר
        analysis: { // נתונים לטרמינל הלייב
            volatility: (parseFloat(k.h) - parseFloat(k.l)).toFixed(2),
            volume: parseFloat(k.v).toFixed(2),
            buyPressure: (price > (parseFloat(k.h) + parseFloat(k.l)) / 2) ? 'HIGH' : 'LOW'
        }
    };

    marketData.finalScore = lockedDecision ? lockedDecision.score : calculateFinalScore(marketData, currentPriceForScore, vwapVal, bb);
    io.emit('marketUpdate', marketData);

    if (isFinal) {
        const finalPrice = indexPrice || price;
        const win = (finalPrice > lastClosePrice); 
        const predictionWasYes = (marketData.finalScore > 50); 
        
        history.unshift({
            time: new Date().toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit' }),
            prediction: predictionWasYes ? 'YES' : 'NO',
            result: win ? 'YES' : 'NO',
            success: (predictionWasYes === win)
        });

        if (history.length > 10) history.pop(); 

        lastClosePrice = finalPrice; 
        lockedDecision = null; // איפוס סופי לסבב הבא

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
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
let prices1m = []; // מערך חדש לגרף דקה
let candles = [];
let history = []; 
let lockedDecision = null; 

// פונקציה למשיכת מחיר מדד מדויק (Coinbase)
async function getPolymarketPrice() {
    try {
        const response = await axios.get('https://api.coinbase.com/v2/prices/BTC-USD/spot');
        return parseFloat(response.data.data.amount);
    } catch (error) {
        try {
            const pythPriceId = "0xe62df6c8b4a8599688d2590a39ec7847776324831ff3f73f35558f07aa9ca2bc";
            const pythRes = await axios.get(`https://hermes.pyth.network/v2/updates/price/latest?ids[]=${pythPriceId}`);
            const p = pythRes.data.parsed[0].price;
            return parseFloat(p.price) * Math.pow(10, p.expo);
        } catch (e) {
            return null;
        }
    }
}

// --- חיבור לסטרים של דקה אחת (1m) לניתוח מהיר ---
const binance1mWS = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@kline_1m');
binance1mWS.on('message', (data) => {
    const msg = JSON.parse(data);
    prices1m.push(parseFloat(msg.k.c));
    if (prices1m.length > 100) prices1m.shift();
});

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

    const indexPrice = await getPolymarketPrice();

    if (!lastClosePrice || secondsLeft >= 299) {
        lastClosePrice = indexPrice || parseFloat(k.o);
        lockedDecision = null; 
    }

    prices.push(price);
    if (prices.length > 200) prices.shift();

    const rsiVal = calculateRSI(prices);
    const rsi1mVal = calculateRSI(prices1m); // RSI של דקה אחת
    const vwapVal = calculateVWAP(k);
    const bb = calculateBollinger(prices);
    const fvg = detectFVG(k);

    const currentPriceForScore = indexPrice || price;

    // --- לוגיקת נעילת החלטה ב-2.5 דקות (150 שניות לסיום) ---
    if (secondsLeft <= 150 && !lockedDecision) {
        // שקלול Score עם נתוני 1m
        let scoreAtLock = calculateFinalScore({lastClose: lastClosePrice, secondsLeft: secondsLeft, fvg: fvg, rsi: rsiVal, trend: currentPriceForScore > vwapVal ? 'BULLISH' : 'BEARISH'}, currentPriceForScore, vwapVal, bb);
        
        // בונוס/קנס אם גרף דקה (1m) מסכים עם גרף 5 דקות
        if (rsiVal > 50 && rsi1mVal > 50) scoreAtLock += 10;
        if (rsiVal < 50 && rsi1mVal < 50) scoreAtLock -= 10;

        lockedDecision = {
            score: Math.min(99, Math.max(1, scoreAtLock)),
            action: scoreAtLock >= 75 ? 'STRONG YES' : scoreAtLock <= 25 ? 'STRONG NO' : 'NEUTRAL',
            time: new Date().toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
        };
    }

    const marketData = {
        price: price,
        indexPrice: indexPrice,
        lastClose: lastClosePrice,
        secondsLeft: secondsLeft,
        rsi: rsiVal,
        rsi1m: rsi1mVal, // נשלח לאתר להצגה בטרמינל
        fvg: fvg,
        trend: currentPriceForScore > vwapVal ? 'BULLISH' : 'BEARISH',
        candle: { o: k.o, h: k.h, l: k.l, c: k.c },
        history: history,
        lockedDecision: lockedDecision, 
        analysis: { 
            volatility: (parseFloat(k.h) - parseFloat(k.l)).toFixed(2),
            volume: parseFloat(k.v).toFixed(2),
            buyPressure: (currentPriceForScore > (parseFloat(k.h) + parseFloat(k.l)) / 2) ? 'HIGH' : 'LOW'
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
        lockedDecision = null; 

        candles.push({ h: parseFloat(k.h), l: parseFloat(k.l), c: price });
        if (candles.length > 20) candles.shift();
    }
});

function calculateFinalScore(data, price, vwap, bb) {
    const diff = price - data.lastClose;
    const timeLeft = data.secondsLeft;
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

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 BEAST ENGINE ACTIVE ON PORT ${PORT}`);
});
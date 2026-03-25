const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');
const axios = require('axios');
const { RSI, VWAP } = require('technicalindicators');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

let lastClosePrice = 0;
let currentLiveIndex = 0;
let dailyStats = { wins: 0, losses: 0, total: 0, rate: 0 };
let prices = [];
let prices1m = []; 
let candles = [];
let history = []; 
let lockedDecision = null; 

// --- סנכרון מחיר "על הסנט" (Coinbase XBX Index) ---
async function syncPolymarketPrice() {
    try {
        // פולימרקט משתמשים במדד ה-XBX של Coinbase
        const response = await axios.get('https://api.coinbase.com/v2/prices/BTC-USD/spot');
        currentLiveIndex = parseFloat(response.data.data.amount);
    } catch (e) {
        // גיבוי ל-Pyth אם Coinbase לא זמין
        try {
            const pythPriceId = "0xe62df6c8b4a8599688d2590a39ec7847776324831ff3f73f35558f07aa9ca2bc";
            const pythRes = await axios.get(`https://hermes.pyth.network/v2/updates/price/latest?ids[]=${pythPriceId}`);
            const p = pythRes.data.parsed[0].price;
            currentLiveIndex = parseFloat(p.price) * Math.pow(10, p.expo);
        } catch (err) {}
    }
    setTimeout(syncPolymarketPrice, 1000); 
}
syncPolymarketPrice();

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
    const price = currentLiveIndex || parseFloat(k.c); 
    const isFinal = k.x;

    const now = Date.now();
    const msIn5Min = 5 * 60 * 1000;
    const nextBoundary = Math.ceil(now / msIn5Min) * msIn5Min;
    const secondsLeft = Math.floor((nextBoundary - now) / 1000);

    if (!lastClosePrice || secondsLeft >= 299) {
        lastClosePrice = currentLiveIndex || parseFloat(k.o);
        lockedDecision = null; 
    }

    prices.push(price);
    if (prices.length > 200) prices.shift();

    const rsiVal = calculateRSI(prices);
    const rsi1mVal = calculateRSI(prices1m);
    const vwapVal = (parseFloat(k.h) + parseFloat(k.l) + parseFloat(k.c)) / 3;
    const fvg = detectFVG(k);
    
    const low = parseFloat(k.l);
    const high = parseFloat(k.h);
    const isLiquiditySweep = (price > low * 1.0001 && low < (candles.length > 0 ? candles[candles.length-1].l : low));

    const analysisResult = calculateSMCScore({
        lastClose: lastClosePrice, 
        secondsLeft: secondsLeft, 
        fvg: fvg, 
        rsi: rsiVal, 
        rsi1m: rsi1mVal,
        trend: price > vwapVal ? 'BULLISH' : 'BEARISH',
        sweep: isLiquiditySweep
    }, price);

    if (secondsLeft <= 150 && !lockedDecision) {
        lockedDecision = {
            score: analysisResult.score,
            action: analysisResult.score >= 50 ? 'YES' : 'NO',
            time: new Date().toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
            reason: analysisResult.signal,
            // שמירת מצב הניתוח ברגע הנעילה לבדיקה עתידית
            fvgAtLock: fvg,
            rsiAtLock: rsiVal,
            trendAtLock: price > vwapVal ? 'BULLISH' : 'BEARISH'
        };
    }

    const marketData = {
        price: price,
        indexPrice: currentLiveIndex,
        lastClose: lastClosePrice,
        secondsLeft: secondsLeft,
        rsi: rsiVal,
        rsi1m: rsi1mVal,
        fvg: fvg,
        trend: price > vwapVal ? 'BULLISH' : 'BEARISH',
        candle: { o: k.o, h: k.h, l: k.l, c: k.c },
        history: history,
        lockedDecision: lockedDecision, 
        smcSignal: analysisResult.signal,
        stats: dailyStats,
        analysis: { 
            volatility: (high - low).toFixed(2),
            volume: parseFloat(k.v).toFixed(2),
            buyPressure: (price > (high + low) / 2) ? 'HIGH' : 'LOW'
        }
    };

    marketData.finalScore = analysisResult.score;
    io.emit('marketUpdate', marketData);

    if (isFinal) {
        const finalPrice = currentLiveIndex || price;
        const actualWin = (finalPrice > lastClosePrice); 
        const predictionWasYes = (marketData.finalScore > 50); 
        const isSuccess = (predictionWasYes === actualWin);

        dailyStats.total++;
        if (isSuccess) dailyStats.wins++; else dailyStats.losses++;
        dailyStats.rate = ((dailyStats.wins / dailyStats.total) * 100).toFixed(1);

        // פירוט אילו ניתוחים צדקו
        const details = {
            fvg: (fvg === 'BULLISH' && actualWin) || (fvg === 'BEARISH' && !actualWin) || (fvg === 'NONE'),
            rsi: (rsiVal > 50 && actualWin) || (rsiVal < 50 && !actualWin),
            trend: (marketData.trend === 'BULLISH' && actualWin) || (marketData.trend === 'BEARISH' && !actualWin)
        };

        history.unshift({
            time: new Date().toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit' }),
            prediction: predictionWasYes ? 'YES' : 'NO',
            result: actualWin ? 'YES' : 'NO',
            closePrice: finalPrice.toFixed(2), // מחיר הסגירה
            success: isSuccess,
            analysisDetails: details
        });

        if (history.length > 10) history.pop(); 
        lastClosePrice = finalPrice; 
        lockedDecision = null; 

        candles.push({ h: high, l: low, c: price });
        if (candles.length > 20) candles.shift();
    }
});

function calculateSMCScore(data, price) {
    let score = 50;
    let signalParts = [];
    const diff = price - data.lastClose;

    score += (diff * 1.5); 

    if (data.fvg !== 'NONE') {
        score += (data.fvg === 'BULLISH' ? 15 : -15);
        signalParts.push(`GAP ${data.fvg}`);
    }

    if (data.rsi > 55 && data.rsi1m > 55) { score += 10; signalParts.push("MOMENTUM 🟢"); }
    if (data.rsi < 45 && data.rsi1m < 45) { score -= 10; signalParts.push("MOMENTUM 🔴"); }
    if (data.sweep) { score += 10; signalParts.push("SWEEP ✅"); }

    return {
        score: Math.min(99, Math.max(1, Math.round(score))),
        signal: signalParts.length > 0 ? signalParts.join(" | ") : "SCANNING SMC..."
    };
}

function calculateRSI(values) {
    if (values.length < 14) return 50;
    const result = RSI.calculate({ values: values.slice(-15), period: 14 });
    return result[result.length - 1] || 50;
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
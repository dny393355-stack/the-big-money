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
let currentLiveIndex = 0; // מחיר מדד חי ומסונכרן
let prices = [];
let prices1m = []; 
let candles = [];
let history = []; 
let lockedDecision = null; 

// --- מנגנון סנכרון רציף למחיר פולימרקט (Coinbase XBX) ---
async function syncPolymarketPrice() {
    try {
        const response = await axios.get('https://api.coinbase.com/v2/prices/BTC-USD/spot');
        currentLiveIndex = parseFloat(response.data.data.amount);
    } catch (e) {
        // גיבוי ל-Pyth אם Coinbase נכשל
        try {
            const pythPriceId = "0xe62df6c8b4a8599688d2590a39ec7847776324831ff3f73f35558f07aa9ca2bc";
            const pythRes = await axios.get(`https://hermes.pyth.network/v2/updates/price/latest?ids[]=${pythPriceId}`);
            const p = pythRes.data.parsed[0].price;
            currentLiveIndex = parseFloat(p.price) * Math.pow(10, p.expo);
        } catch (err) {}
    }
    setTimeout(syncPolymarketPrice, 2000); // עדכון כל 2 שניות לדיוק מקסימלי
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
    const price = currentLiveIndex || parseFloat(k.c); // שימוש במדד המסונכרן כמחיר ראשי
    const isFinal = k.x;

    const now = Date.now();
    const msIn5Min = 5 * 60 * 1000;
    const nextBoundary = Math.ceil(now / msIn5Min) * msIn5Min;
    const secondsLeft = Math.floor((nextBoundary - now) / 1000);

    // נעילת מחיר יעד בתחילת סבב
    if (!lastClosePrice || secondsLeft >= 299) {
        lastClosePrice = currentLiveIndex || parseFloat(k.o);
        lockedDecision = null; 
    }

    prices.push(price);
    if (prices.length > 200) prices.shift();

    const rsiVal = calculateRSI(prices);
    const rsi1mVal = calculateRSI(prices1m);
    const vwapVal = calculateVWAP(k);
    const fvg = detectFVG(k);
    
    const low = parseFloat(k.l);
    const high = parseFloat(k.h);
    const isLiquiditySweep = (price > low * 1.0001 && low < (candles.length > 0 ? candles[candles.length-1].l : low));

    // ניתוח SMC מורחב
    const analysisResult = calculateSMCScore({
        lastClose: lastClosePrice, 
        secondsLeft: secondsLeft, 
        fvg: fvg, 
        rsi: rsiVal, 
        rsi1m: rsi1mVal,
        trend: price > vwapVal ? 'BULLISH' : 'BEARISH',
        sweep: isLiquiditySweep
    }, price, vwapVal);

    // נעילת החלטה ב-2.5 דקות
    if (secondsLeft <= 150 && !lockedDecision) {
        lockedDecision = {
            score: analysisResult.score,
            action: analysisResult.score >= 70 ? 'STRONG YES' : analysisResult.score <= 30 ? 'STRONG NO' : 'NEUTRAL',
            time: new Date().toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
            reason: analysisResult.signal
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
        analysis: { 
            volatility: (high - low).toFixed(2),
            volume: parseFloat(k.v).toFixed(2),
            buyPressure: (price > (high + low) / 2) ? 'HIGH' : 'LOW'
        }
    };

    marketData.finalScore = lockedDecision ? lockedDecision.score : analysisResult.score;
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
        lastClosePrice = price; 
        lockedDecision = null; 

        candles.push({ h: high, l: low, c: price });
        if (candles.length > 20) candles.shift();
    }
});

function calculateSMCScore(data, price, vwap) {
    let score = 50;
    let signalParts = [];
    const diff = price - data.lastClose;

    // ניתוח FVG ופריצה מקדימה
    if (data.fvg !== 'NONE') {
        score += (data.fvg === 'BULLISH' ? 20 : -20);
        signalParts.push(`PRE-ANALYSIS: ${data.fvg} GAP`);
    }

    if (data.rsi > 58 && data.rsi1m > 58) {
        score += 15;
        signalParts.push("DUAL MOMENTUM 🟢");
    } else if (data.rsi < 42 && data.rsi1m < 42) {
        score -= 15;
        signalParts.push("DUAL MOMENTUM 🔴");
    }

    if (data.sweep) {
        score += 12;
        signalParts.push("LIQ SWEEP ✅");
    }

    if (data.trend === 'BULLISH') score += 8; else score -= 8;

    // לחץ מחיר בסוף סבב
    if (data.secondsLeft < 100) {
        if (diff > 10) score += 10;
        if (diff < -10) score -= 10;
    }

    return {
        score: Math.min(99, Math.max(1, Math.round(score))),
        signal: signalParts.length > 0 ? signalParts.join(" | ") : "SCANNING SMC STRUCTURE..."
    };
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
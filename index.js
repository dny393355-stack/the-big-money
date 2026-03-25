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
let prices1m = []; 
let candles = [];
let history = []; 
let lockedDecision = null; 

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
    const rsi1mVal = calculateRSI(prices1m);
    const vwapVal = calculateVWAP(k);
    const fvg = detectFVG(k);
    
    const low = parseFloat(k.l);
    const high = parseFloat(k.h);
    const isLiquiditySweep = (price > low * 1.0001 && low < (candles.length > 0 ? candles[candles.length-1].l : low));

    const currentPriceForScore = indexPrice || price;

    // קבלת הניתוח המפורט
    const analysisResult = calculateSMCScore({
        lastClose: lastClosePrice, 
        secondsLeft: secondsLeft, 
        fvg: fvg, 
        rsi: rsiVal, 
        rsi1m: rsi1mVal,
        trend: currentPriceForScore > vwapVal ? 'BULLISH' : 'BEARISH',
        sweep: isLiquiditySweep
    }, currentPriceForScore, vwapVal);

    if (secondsLeft <= 150 && !lockedDecision) {
        lockedDecision = {
            score: analysisResult.score,
            action: analysisResult.score >= 70 ? 'STRONG YES' : analysisResult.score <= 30 ? 'STRONG NO' : 'NEUTRAL',
            time: new Date().toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
            reason: analysisResult.signal // הסיבה שננעלת
        };
    }

    const marketData = {
        price: price,
        indexPrice: indexPrice,
        lastClose: lastClosePrice,
        secondsLeft: secondsLeft,
        rsi: rsiVal,
        rsi1m: rsi1mVal,
        fvg: fvg,
        trend: currentPriceForScore > vwapVal ? 'BULLISH' : 'BEARISH',
        candle: { o: k.o, h: k.h, l: k.l, c: k.c },
        history: history,
        lockedDecision: lockedDecision, 
        smcSignal: analysisResult.signal, // שליחת הסיבה בלייב
        analysis: { 
            volatility: (high - low).toFixed(2),
            volume: parseFloat(k.v).toFixed(2),
            buyPressure: (currentPriceForScore > (high + low) / 2) ? 'HIGH' : 'LOW'
        }
    };

    marketData.finalScore = lockedDecision ? lockedDecision.score : analysisResult.score;
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

        candles.push({ h: high, l: low, c: price });
        if (candles.length > 20) candles.shift();
    }
});

function calculateSMCScore(data, price, vwap) {
    let score = 50;
    let signalParts = [];
    const diff = price - data.lastClose;

    if (data.fvg !== 'NONE') {
        score += (data.fvg === 'BULLISH' ? 20 : -20);
        signalParts.push(`FVG ${data.fvg}`);
    }

    if (data.rsi > 55 && data.rsi1m > 55) {
        score += 15;
        signalParts.push("DUAL MOMENTUM 🟢");
    } else if (data.rsi < 45 && data.rsi1m < 45) {
        score -= 15;
        signalParts.push("DUAL MOMENTUM 🔴");
    }

    if (data.sweep) {
        score += 10;
        signalParts.push("LIQ SWEEP ✅");
    }

    if (data.trend === 'BULLISH') score += 10; else score -= 10;

    if (data.secondsLeft < 90) {
        if (Math.abs(diff) > 15) signalParts.push("PRICE PRESSURE");
    }

    return {
        score: Math.min(99, Math.max(1, Math.round(score))),
        signal: signalParts.length > 0 ? signalParts.join(" | ") : "WAITING FOR SMC CONFIRMATION..."
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
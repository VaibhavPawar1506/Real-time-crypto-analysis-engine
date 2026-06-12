import React, { useEffect, useRef, useState } from 'react';
import { Client } from '@stomp/stompjs';
import { createChart, IChartApi, ISeriesApi, Time } from 'lightweight-charts';

interface MarketTick {
    symbol: string;
    price: number;
    volume: number;
    timestamp: number;
}

const CryptoDashboard: React.FC = () => {
    const chartContainerRef = useRef<HTMLDivElement>(null);
    const chartRef = useRef<IChartApi | null>(null);
    const seriesRef = useRef<ISeriesApi<"Line"> | null>(null);
    
    // Throttling buffer
    const tickBufferRef = useRef<MarketTick[]>([]);
    const lastUpdateTimeRef = useRef<number>(Date.now());
    
    const [latestPrice, setLatestPrice] = useState<number | null>(null);
    const [connectionStatus, setConnectionStatus] = useState<string>('Connecting...');

    useEffect(() => {
        if (!chartContainerRef.current) return;

        // Initialize Lightweight Chart
        const chart = createChart(chartContainerRef.current, {
            layout: {
                background: { color: '#1E222D' },
                textColor: '#D9D9D9',
            },
            grid: {
                vertLines: { color: '#2B2B43' },
                horzLines: { color: '#2B2B43' },
            },
            width: chartContainerRef.current.clientWidth,
            height: 400,
            timeScale: {
                timeVisible: true,
                secondsVisible: true,
            },
        });
        
        const lineSeries = chart.addLineSeries({
            color: '#2962FF',
            lineWidth: 2,
        });

        chartRef.current = chart;
        seriesRef.current = lineSeries;

        const handleResize = () => {
            if (chartContainerRef.current) {
                chart.applyOptions({ width: chartContainerRef.current.clientWidth });
            }
        };

        window.addEventListener('resize', handleResize);

        return () => {
            window.removeEventListener('resize', handleResize);
            chart.remove();
        };
    }, []);

    useEffect(() => {
        const client = new Client({
            brokerURL: 'ws://localhost:8080/ws', // Aligning with the configured Spring Boot endpoint
            reconnectDelay: 5000,
            onConnect: () => {
                setConnectionStatus('Connected');
                client.subscribe('/topic/live-crypto/BTCUSDT', (message) => {
                    if (message.body) {
                        try {
                            const parsedMessage = JSON.parse(message.body);
                            
                            // Support unpacking from both AnalyticsSnapshot or direct MarketTick formats
                            const tick: MarketTick = parsedMessage.latestTick || parsedMessage;
                            
                            // Buffer the tick for throttled rendering
                            tickBufferRef.current.push(tick);
                        } catch (e) {
                            console.error("Failed to parse STOMP message", e);
                        }
                    }
                });
            },
            onDisconnect: () => {
                setConnectionStatus('Disconnected');
            },
            onStompError: (frame) => {
                console.error('Broker reported error: ' + frame.headers['message']);
                console.error('Additional details: ' + frame.body);
            },
        });

        client.activate();

        return () => {
            client.deactivate();
        };
    }, []);

    useEffect(() => {
        // Throttling loop checking every 100ms
        const intervalId = setInterval(() => {
            const now = Date.now();
            // Perform flush strictly once every 300ms
            if (now - lastUpdateTimeRef.current >= 300) {
                const buffer = tickBufferRef.current;
                if (buffer.length > 0) {
                    // Update chart with all buffered ticks maintaining chronological order
                    const sortedTicks = [...buffer].sort((a, b) => a.timestamp - b.timestamp);
                    
                    if (seriesRef.current) {
                        sortedTicks.forEach(tick => {
                            seriesRef.current?.update({
                                time: (tick.timestamp / 1000) as Time,
                                value: tick.price,
                            });
                        });
                    }

                    // Flush UI state with the most recent price
                    const lastTick = sortedTicks[sortedTicks.length - 1];
                    setLatestPrice(lastTick.price);

                    // Reset buffer
                    tickBufferRef.current = [];
                    lastUpdateTimeRef.current = now;
                }
            }
        }, 100);

        return () => clearInterval(intervalId);
    }, []);

    return (
        <div className="min-h-screen bg-gray-900 text-white p-8 flex flex-col items-center font-sans">
            <header className="mb-8 w-full max-w-5xl flex justify-between items-center">
                <h1 className="text-3xl font-bold text-blue-400">Crypto Analytics Engine</h1>
                <div className="flex items-center gap-4">
                    <span className="text-gray-400 text-sm font-medium flex items-center">
                        <span className="mr-2">Status:</span>
                        <span className={`px-2 py-1 rounded text-xs font-bold ${
                            connectionStatus === 'Connected' ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'
                        }`}>
                            {connectionStatus}
                        </span>
                    </span>
                    <div className="bg-gray-800 px-6 py-2 rounded-lg border border-gray-700 shadow-sm flex items-center">
                        <span className="text-gray-400 text-sm mr-3 font-semibold">BTC/USDT</span>
                        <span className="text-2xl font-mono font-semibold text-green-400 min-w-[120px] text-right">
                            {latestPrice ? `$${latestPrice.toFixed(2)}` : '---'}
                        </span>
                    </div>
                </div>
            </header>

            <main className="w-full max-w-5xl bg-gray-800 rounded-xl shadow-2xl border border-gray-700 overflow-hidden">
                <div className="p-4 border-b border-gray-700 flex justify-between items-center bg-gray-800/50">
                    <h2 className="text-lg font-medium text-gray-200">Real-Time Price Chart</h2>
                    <div className="flex items-center gap-2">
                        <div className="w-2 h-2 rounded-full bg-blue-500 animate-pulse"></div>
                        <span className="text-xs text-blue-400 font-mono tracking-wider">THROTTLED (300MS)</span>
                    </div>
                </div>
                {/* Chart Injection Point */}
                <div ref={chartContainerRef} className="w-full h-[400px]" />
            </main>
        </div>
    );
};

export default CryptoDashboard;

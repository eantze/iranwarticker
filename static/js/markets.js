// Markets Dashboard - Yahoo Finance charts with customizable date range and strait marker

const DEFAULT_START = "2026-01-01";
const DEFAULT_STRAIT = "2026-03-01";
const SHARED_STRAIT_KEY = "shared_strait_date";
const MARKETS_SETTINGS_KEY = "markets_settings";

let startDate = DEFAULT_START;
let straitDate = DEFAULT_STRAIT;
let rawData = null;  // Full data from API

function loadSettings() {
    // Load shared strait date (persists across tabs)
    try {
        const shared = localStorage.getItem(SHARED_STRAIT_KEY);
        if (shared) straitDate = shared;
    } catch (e) {}

    // Load markets-specific start date
    try {
        const saved = localStorage.getItem(MARKETS_SETTINGS_KEY);
        if (saved) {
            const s = JSON.parse(saved);
            if (s.startDate) startDate = s.startDate;
        }
    } catch (e) {}

    // Also check if Oil & Gas page or Environmental page has set a date
    try {
        const tickerSettings = localStorage.getItem("ticker_settings");
        if (tickerSettings) {
            const s = JSON.parse(tickerSettings);
            if (s.date) {
                straitDate = s.date;
                localStorage.setItem(SHARED_STRAIT_KEY, s.date);
            }
        }
    } catch (e) {}

    try {
        const envSettings = localStorage.getItem("env_ticker_settings");
        if (envSettings) {
            const s = JSON.parse(envSettings);
            if (s.date) {
                straitDate = s.date;
                localStorage.setItem(SHARED_STRAIT_KEY, s.date);
            }
        }
    } catch (e) {}

    // Set input values
    document.getElementById("input-start-date").value = startDate;
    document.getElementById("input-strait-date").value = straitDate;
    updateSubtitle();
}

function saveSettings() {
    localStorage.setItem(MARKETS_SETTINGS_KEY, JSON.stringify({ startDate }));
    localStorage.setItem(SHARED_STRAIT_KEY, straitDate);

    // Also update the other pages' settings so strait date persists
    try {
        const ticker = localStorage.getItem("ticker_settings");
        if (ticker) {
            const s = JSON.parse(ticker);
            s.date = straitDate;
            localStorage.setItem("ticker_settings", JSON.stringify(s));
        }
    } catch (e) {}

    try {
        const env = localStorage.getItem("env_ticker_settings");
        if (env) {
            const s = JSON.parse(env);
            s.date = straitDate;
            localStorage.setItem("env_ticker_settings", JSON.stringify(s));
        }
    } catch (e) {}
}

function updateSubtitle() {
    const el = document.getElementById("dash-subtitle");
    if (el) {
        const start = new Date(startDate + "T00:00:00Z");
        const startStr = start.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" });
        el.textContent = `${startStr} — Present`;
    }
}

function applyControls() {
    const newStart = document.getElementById("input-start-date").value;
    const newStrait = document.getElementById("input-strait-date").value;

    if (newStart && newStart >= "2026-01-01") startDate = newStart;
    if (newStrait && newStrait >= "2026-01-01") straitDate = newStrait;

    saveSettings();
    updateSubtitle();

    if (rawData) renderDashboard(rawData);
}

function resetControls() {
    startDate = DEFAULT_START;
    straitDate = DEFAULT_STRAIT;

    document.getElementById("input-start-date").value = DEFAULT_START;
    document.getElementById("input-strait-date").value = DEFAULT_STRAIT;

    localStorage.removeItem(MARKETS_SETTINGS_KEY);
    localStorage.removeItem(SHARED_STRAIT_KEY);
    updateSubtitle();

    if (rawData) renderDashboard(rawData);
}

// Make these available globally for onclick
window.applyControls = applyControls;
window.resetControls = resetControls;

async function fetchPrices() {
    const resp = await fetch("/api/prices");
    return resp.json();
}

function formatPrice(price, key) {
    if (price == null) return "--";
    if (price >= 1000) return price.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return price.toFixed(2);
}

function filterHistory(history) {
    if (!history) return [];
    return history.filter(d => d.date >= startDate);
}

function buildChart(key, instrument) {
    const container = document.getElementById(`chartbody-${key}`);
    if (!container) return;

    const history = filterHistory(instrument.history);
    if (history.length < 2) {
        container.innerHTML = '<div class="chart-loading">Loading data...</div>';
        return;
    }

    const W = 560;
    const H = 220;
    const PAD_L = 60;
    const PAD_R = 20;
    const PAD_T = 20;
    const PAD_B = 40;
    const chartW = W - PAD_L - PAD_R;
    const chartH = H - PAD_T - PAD_B;

    const closes = history.map(d => d.close);
    const dates = history.map(d => d.date);
    const min = Math.min(...closes);
    const max = Math.max(...closes);
    const range = max - min || 1;

    const yMin = min - range * 0.05;
    const yMax = max + range * 0.05;
    const yRange = yMax - yMin;

    function xPos(i) { return PAD_L + (i / (closes.length - 1)) * chartW; }
    function yPos(v) { return PAD_T + (1 - (v - yMin) / yRange) * chartH; }

    const linePts = closes.map((v, i) => `${xPos(i)},${yPos(v)}`).join(" ");
    const areaPts = `${xPos(0)},${PAD_T + chartH} ${linePts} ${xPos(closes.length - 1)},${PAD_T + chartH}`;

    // Find strait closure marker position
    let markerX = null;
    let markerIdx = dates.findIndex(d => d >= straitDate);
    if (markerIdx >= 0) {
        markerX = xPos(markerIdx);
    }

    // Format the strait date for label
    const straitD = new Date(straitDate + "T00:00:00Z");
    const straitLabel = straitD.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });

    // Determine overall trend color
    const first = closes[0];
    const last = closes[closes.length - 1];
    const isUp = last >= first;
    const lineColor = isUp ? "#00e5a0" : "#ff4d6a";

    // Y-axis labels (5 ticks)
    let yLabels = "";
    for (let i = 0; i <= 4; i++) {
        const val = yMin + (i / 4) * yRange;
        const y = yPos(val);
        const label = val >= 1000 ? val.toLocaleString("en-US", { maximumFractionDigits: 0 }) : val.toFixed(2);
        yLabels += `<text x="${PAD_L - 8}" y="${y + 4}" text-anchor="end" class="axis-label">${label}</text>`;
        yLabels += `<line x1="${PAD_L}" y1="${y}" x2="${W - PAD_R}" y2="${y}" class="grid-line"/>`;
    }

    // X-axis date labels
    let xLabels = "";
    xLabels += `<text x="${PAD_L}" y="${H - 8}" text-anchor="start" class="axis-label">${dates[0]}</text>`;
    xLabels += `<text x="${W - PAD_R}" y="${H - 8}" text-anchor="end" class="axis-label">${dates[dates.length - 1]}</text>`;

    // Strait closure marker
    let markerSvg = "";
    if (markerX !== null) {
        markerSvg = `
            <line x1="${markerX}" y1="${PAD_T}" x2="${markerX}" y2="${PAD_T + chartH}"
                class="march1-line"/>
            <text x="${markerX}" y="${H - 8}" text-anchor="middle" class="march1-label">${straitLabel}</text>
            <text x="${markerX + 4}" y="${PAD_T + 14}" text-anchor="start" class="march1-tag">Strait Closed</text>
        `;
    }

    const svg = `
        <svg width="100%" viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet" class="market-chart"
             id="svg-${key}">
            <defs>
                <linearGradient id="grad-${key}" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stop-color="${lineColor}" stop-opacity="0.15"/>
                    <stop offset="100%" stop-color="${lineColor}" stop-opacity="0.01"/>
                </linearGradient>
            </defs>

            ${yLabels}

            <polygon points="${areaPts}" fill="url(#grad-${key})"/>

            <polyline points="${linePts}" fill="none" stroke="${lineColor}"
                stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>

            ${markerSvg}
            ${xLabels}

            <rect x="${PAD_L}" y="${PAD_T}" width="${chartW}" height="${chartH}"
                fill="transparent" class="hover-zone" data-key="${key}" />

            <line id="hover-line-${key}" x1="0" y1="${PAD_T}" x2="0" y2="${PAD_T + chartH}"
                class="hover-crosshair" style="display:none"/>
            <circle id="hover-dot-${key}" r="4" fill="${lineColor}" stroke="#0a0a1a" stroke-width="2"
                style="display:none"/>
            <g id="hover-tooltip-${key}" style="display:none">
                <rect id="hover-bg-${key}" rx="4" ry="4" class="tooltip-bg"/>
                <text id="hover-text-${key}" class="tooltip-text"></text>
                <text id="hover-date-${key}" class="tooltip-date"></text>
            </g>
        </svg>
    `;

    container.innerHTML = svg;

    // Attach hover events
    const svgEl = document.getElementById(`svg-${key}`);
    const hoverZone = svgEl.querySelector(".hover-zone");

    hoverZone.addEventListener("mousemove", (e) => {
        const rect = svgEl.getBoundingClientRect();
        const svgW = rect.width;
        const scaleX = W / svgW;
        const mouseX = (e.clientX - rect.left) * scaleX;

        const idx = Math.round(((mouseX - PAD_L) / chartW) * (closes.length - 1));
        const clampedIdx = Math.max(0, Math.min(closes.length - 1, idx));

        const px = xPos(clampedIdx);
        const py = yPos(closes[clampedIdx]);
        const val = closes[clampedIdx];
        const date = dates[clampedIdx];

        const line = document.getElementById(`hover-line-${key}`);
        const dot = document.getElementById(`hover-dot-${key}`);
        const tooltip = document.getElementById(`hover-tooltip-${key}`);
        const bg = document.getElementById(`hover-bg-${key}`);
        const text = document.getElementById(`hover-text-${key}`);
        const dateText = document.getElementById(`hover-date-${key}`);

        line.setAttribute("x1", px);
        line.setAttribute("x2", px);
        line.style.display = "";

        dot.setAttribute("cx", px);
        dot.setAttribute("cy", py);
        dot.style.display = "";

        const valStr = formatPrice(val, key);
        text.textContent = valStr;
        dateText.textContent = date;

        const ttX = px < W / 2 ? px + 12 : px - 100;
        const ttY = Math.max(PAD_T, py - 30);
        text.setAttribute("x", ttX + 6);
        text.setAttribute("y", ttY + 14);
        dateText.setAttribute("x", ttX + 6);
        dateText.setAttribute("y", ttY + 28);
        bg.setAttribute("x", ttX);
        bg.setAttribute("y", ttY);
        bg.setAttribute("width", 88);
        bg.setAttribute("height", 34);
        tooltip.style.display = "";
    });

    hoverZone.addEventListener("mouseleave", () => {
        document.getElementById(`hover-line-${key}`).style.display = "none";
        document.getElementById(`hover-dot-${key}`).style.display = "none";
        document.getElementById(`hover-tooltip-${key}`).style.display = "none";
    });
}

function updateQuoteDisplay(key, instrument) {
    const el = document.getElementById(`quote-${key}`);
    if (!el) return;

    if (instrument.price == null) {
        el.innerHTML = '<span class="quote-loading">--</span>';
        return;
    }

    const price = formatPrice(instrument.price, key);
    const change = instrument.change;
    const changePct = instrument.change_pct;
    const isUp = change >= 0;
    const color = isUp ? "#00e5a0" : "#ff4d6a";
    const arrow = isUp ? "▲" : "▼";

    // Calculate change since start of visible range
    const filteredHistory = filterHistory(instrument.history);
    let sinceStart = "";
    if (filteredHistory.length > 0) {
        const first = filteredHistory[0].close;
        const pct = ((instrument.price - first) / first * 100);
        const startD = new Date(startDate + "T00:00:00Z");
        const label = startD.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
        sinceStart = `<span class="quote-ytd" style="color: ${pct >= 0 ? '#00e5a0' : '#ff4d6a'}">
            Since ${label}: ${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%
        </span>`;
    }

    el.innerHTML = `
        <span class="quote-price">${price}</span>
        <span class="quote-change" style="color: ${color}">
            ${arrow} ${change >= 0 ? '+' : ''}${Math.abs(change).toFixed(2)} (${changePct})
        </span>
        ${sinceStart}
    `;
}

function renderDashboard(data) {
    rawData = data;
    const keys = ["sp500", "dji", "tyx", "wti", "brent"];
    for (const key of keys) {
        const instrument = data[key];
        if (!instrument) continue;
        updateQuoteDisplay(key, instrument);
        buildChart(key, instrument);
    }

    const tsEl = document.getElementById("last-updated");
    if (tsEl && data.updated_at) {
        const d = new Date(data.updated_at * 1000);
        tsEl.textContent = `Last updated: ${d.toLocaleTimeString()}`;
    }
}

function updateClock() {
    const el = document.getElementById("dash-clock");
    if (el) {
        const now = new Date();
        el.textContent = now.toLocaleTimeString("en-US", { hour12: false });
    }
}

function updateDate() {
    const el = document.getElementById("dash-date");
    if (el) {
        const now = new Date();
        el.textContent = now.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });
    }
}

document.addEventListener("DOMContentLoaded", async () => {
    loadSettings();
    updateClock();
    updateDate();
    setInterval(updateClock, 1000);

    try {
        const data = await fetchPrices();
        renderDashboard(data);
    } catch (e) {
        console.error("Failed to fetch prices:", e);
    }

    // Refresh every 5 minutes
    setInterval(async () => {
        try {
            const data = await fetchPrices();
            renderDashboard(data);
        } catch (e) {
            console.error("Failed to fetch prices:", e);
        }
    }, 300000);
});

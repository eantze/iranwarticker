// Markets Dashboard - fetch, render, poll

let lastPrices = {};

async function fetchPrices() {
    const resp = await fetch("/api/prices");
    return resp.json();
}

function formatPrice(price, key) {
    if (price == null) return "--";
    if (key === "rbob") return price.toFixed(4);
    if (key === "ng") return price.toFixed(2);
    if (price >= 1000) return price.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return price.toFixed(2);
}

function getUnit(key) {
    if (key === "sp500" || key === "dji") return "pts";
    if (key === "wti" || key === "brent") return "$/barrel";
    if (key === "rbob") return "$/gallon";
    if (key === "ng") return "$/MMBtu";
    return "";
}

function getFrequency(key) {
    if (key === "sp500" || key === "dji") return "Daily";
    return "Hourly";
}

function sparklineSVG(data, color, key, w, h) {
    let closes;
    if (key === "sp500" || key === "dji") {
        closes = data.map(d => d.close);
    } else {
        closes = data.map(d => d.close);
    }

    if (closes.length < 2) return "";

    const min = Math.min(...closes);
    const max = Math.max(...closes);
    const range = max - min || 0.001;
    const pad = 8;

    const points = closes.map((v, i) => {
        const x = (i / (closes.length - 1)) * w;
        const y = pad + (1 - (v - min) / range) * (h - pad * 2);
        return `${x},${y}`;
    }).join(" ");

    const lastY = pad + (1 - (closes[closes.length - 1] - min) / range) * (h - pad * 2);
    const gid = "g" + Math.random().toString(36).slice(2, 7);

    return `
        <svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
            <defs>
                <linearGradient id="${gid}" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stop-color="${color}" stop-opacity="0.18"/>
                    <stop offset="100%" stop-color="${color}" stop-opacity="0.01"/>
                </linearGradient>
            </defs>
            <polygon points="0,${h} ${points} ${w},${h}" fill="url(#${gid})"/>
            <polyline points="${points}" fill="none" stroke="${color}"
                stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <circle cx="${w}" cy="${lastY}" r="3" fill="${color}"
                stroke="#0a0a1a" stroke-width="2"/>
        </svg>`;
}

function computeChange(history, currentPrice) {
    if (!history || history.length === 0 || currentPrice == null) return null;
    const firstClose = history[0].close;
    if (!firstClose) return null;
    const change = currentPrice - firstClose;
    const changePct = (change / firstClose) * 100;
    return { change, changePct };
}

function renderCard(key, instrument, container) {
    const isUp = (instrument.change != null ? instrument.change >= 0 : true);
    const color = isUp ? "#00e5a0" : "#ff4d6a";
    const arrow = isUp ? "\u25B2" : "\u25BC";

    // Day change for indices, computed change for commodities
    let changeVal, changePctStr;
    if (key === "sp500" || key === "dji") {
        changeVal = instrument.change;
        changePctStr = instrument.change_pct;
    } else {
        const computed = computeChange(instrument.history, instrument.price);
        if (computed) {
            changeVal = computed.change;
            changePctStr = (computed.changePct >= 0 ? "+" : "") + computed.changePct.toFixed(2) + "%";
        }
    }

    // Period change (from start of history)
    let periodStr = "";
    if (instrument.history && instrument.history.length > 0 && instrument.price != null) {
        const first = instrument.history[0].close;
        if (first) {
            const pctFromStart = ((instrument.price - first) / first * 100);
            periodStr = `Mar 1: ${pctFromStart >= 0 ? "+" : ""}${pctFromStart.toFixed(2)}%`;
        }
    }

    const history = instrument.history || [];
    const sparkW = 260;
    const sparkH = 60;
    const svg = sparklineSVG(history, color, key, sparkW, sparkH);

    const startLabel = "Mar 1";
    const pointCount = history.length + " pts";
    const endLabel = "Now";

    const priceFormatted = formatPrice(instrument.price, key);
    const isFlash = lastPrices[key] != null && lastPrices[key] !== instrument.price;

    const card = document.createElement("div");
    card.className = "price-card" + (instrument.price == null ? " card-loading" : "");
    card.style.setProperty("--card-glow", color);
    card.id = `card-${key}`;

    card.innerHTML = `
        <span class="card-exchange">${instrument.exchange}</span>
        <span class="card-freq">${getFrequency(key)}</span>
        <div class="card-label">${instrument.label}</div>
        <div class="card-price-row">
            <span class="card-price ${isFlash ? "price-flash" : ""}" style="--flash-color: ${color}">${priceFormatted}</span>
            <span class="card-unit">${getUnit(key)}</span>
        </div>
        ${changeVal != null ? `
        <div class="card-change ${changeVal >= 0 ? "up" : "down"}">
            ${arrow} ${changeVal >= 0 ? "+" : ""}${Math.abs(changeVal).toFixed(2)} (${changePctStr})
        </div>` : ""}
        ${periodStr ? `<div class="card-period-change">${periodStr}</div>` : ""}
        <div class="card-sparkline">${svg}</div>
        <div class="sparkline-footer">
            <span>${startLabel}</span>
            <span class="count">${pointCount}</span>
            <span>${endLabel}</span>
        </div>
    `;

    lastPrices[key] = instrument.price;

    const existing = document.getElementById(`card-${key}`);
    if (existing) {
        existing.replaceWith(card);
    } else {
        container.appendChild(card);
    }
}

function renderDashboard(data) {
    // Indices
    const indicesGrid = document.getElementById("grid-indices");
    indicesGrid.innerHTML = "";
    for (const [key, inst] of Object.entries(data.indices)) {
        renderCard(key, inst, indicesGrid);
    }

    // Crude
    const crudeGrid = document.getElementById("grid-crude");
    crudeGrid.innerHTML = "";
    for (const [key, inst] of Object.entries(data.crude)) {
        renderCard(key, inst, crudeGrid);
    }

    // Fuel
    const fuelGrid = document.getElementById("grid-fuel");
    fuelGrid.innerHTML = "";
    for (const [key, inst] of Object.entries(data.fuel)) {
        renderCard(key, inst, fuelGrid);
    }

    // Update timestamp
    const tsEl = document.getElementById("last-updated");
    if (tsEl && data.updated_at) {
        const d = new Date(data.updated_at * 1000);
        tsEl.textContent = `Last updated: ${d.toLocaleTimeString()}`;
    }

    // Demo badge
    const demoEl = document.getElementById("demo-badge");
    if (demoEl) {
        demoEl.style.display = data.demo_mode ? "inline" : "none";
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
    updateClock();
    updateDate();
    setInterval(updateClock, 1000);

    try {
        const data = await fetchPrices();
        renderDashboard(data);
    } catch (e) {
        console.error("Failed to fetch prices:", e);
    }

    setInterval(async () => {
        try {
            const data = await fetchPrices();
            renderDashboard(data);
        } catch (e) {
            console.error("Failed to fetch prices:", e);
        }
    }, 30000);
});

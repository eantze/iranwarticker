/* Casualties Dashboard — fetches /api/casualties and renders charts + totals */

const COLORS = {
    us_deaths: "#ff4d6a",
    iran_deaths: "#ff6b81",
    other_deaths: "#ff8fa0",
    us_injuries: "#ffaa32",
    iran_injuries: "#ffc266",
    other_injuries: "#ffd699",
    displaced: "#6496ff",
};

const LABELS = {
    us_deaths: "US Deaths",
    iran_deaths: "Iranian Deaths",
    other_deaths: "Other Deaths",
    us_injuries: "US Injuries",
    iran_injuries: "Iranian Injuries",
    other_injuries: "Other Injuries",
    displaced: "People Displaced",
};

async function fetchCasualties() {
    const resp = await fetch("/api/casualties");
    return resp.json();
}

function formatNumber(n) {
    if (n == null) return "--";
    return n.toLocaleString("en-US");
}

function renderSummaryCards(totals) {
    const totalDeaths = (totals.us_deaths || 0) + (totals.iran_deaths || 0) + (totals.other_deaths || 0);
    const totalInjuries = (totals.us_injuries || 0) + (totals.iran_injuries || 0) + (totals.other_injuries || 0);

    // Deaths — each in its own row
    setSingle("summary-total-deaths", summaryCardHTML("Total Deaths", totalDeaths, "deaths", "deaths-color"));
    setSingle("summary-us-deaths", summaryCardHTML("US Deaths", totals.us_deaths || 0, "deaths", "deaths-color"));
    setSingle("summary-iran-deaths", summaryCardHTML("Iranian Deaths", totals.iran_deaths || 0, "deaths", "deaths-color"));
    setSingle("summary-other-deaths", summaryCardHTML("Other Deaths", totals.other_deaths || 0, "deaths", "deaths-color"));

    // Injuries — each in its own row
    setSingle("summary-total-injuries", summaryCardHTML("Total Injuries", totalInjuries, "injuries", "injuries-color"));
    setSingle("summary-us-injuries", summaryCardHTML("US Injuries", totals.us_injuries || 0, "injuries", "injuries-color"));
    setSingle("summary-iran-injuries", summaryCardHTML("Iranian Injuries", totals.iran_injuries || 0, "injuries", "injuries-color"));
    setSingle("summary-other-injuries", summaryCardHTML("Other Injuries", totals.other_injuries || 0, "injuries", "injuries-color"));

    // Displaced
    setSingle("summary-displaced", summaryCardHTML("People Displaced", totals.displaced || 0, "displaced", "displaced-color"));
}

function setSingle(id, html) {
    const el = document.getElementById(id);
    if (el) el.innerHTML = html;
}

function summaryCardHTML(label, value, type, colorClass) {
    return `
        <div class="summary-card ${type}">
            <div class="summary-label">${label}</div>
            <div class="summary-value ${colorClass}">${formatNumber(value)}</div>
        </div>`;
}

function buildBarChart(data, color, w, h, sharedMax) {
    if (!data || data.length === 0) {
        return `<svg width="${w}" height="${h}"><text x="${w / 2}" y="${h / 2}" text-anchor="middle" fill="rgba(255,255,255,0.2)" font-size="11" font-family="JetBrains Mono, monospace">No data yet</text></svg>`;
    }

    const values = data.map(d => d.value);
    const dates = data.map(d => d.date);
    const max = sharedMax || Math.max(...values, 1);
    const pad = 4;
    const tooltipH = 20;
    const chartH = h - tooltipH;
    const barW = Math.max(2, (w - pad * 2) / values.length - 2);
    const gap = (w - pad * 2 - barW * values.length) / Math.max(values.length - 1, 1);
    const uid = "bc" + Math.random().toString(36).slice(2, 7);

    let bars = "";
    for (let i = 0; i < values.length; i++) {
        const barH = Math.max(1, (values[i] / max) * (chartH - pad * 2));
        const x = pad + i * (barW + gap);
        const y = chartH - pad - barH;
        // Visible bar
        bars += `<rect x="${x}" y="${y}" width="${barW}" height="${barH}" fill="${color}" opacity="0.8" rx="1" class="bar-rect" data-idx="${i}"/>`;
        // Invisible hit area (full height for easier hover)
        bars += `<rect x="${x}" y="0" width="${barW}" height="${chartH}" fill="transparent" class="bar-hit" data-idx="${i}"
            onmouseenter="showBarTooltip('${uid}', ${i}, '${dates[i]}', ${values[i]}, ${x}, ${barW})"
            onmouseleave="hideBarTooltip('${uid}')"/>`;
    }

    // Tooltip group (hidden by default)
    const tooltip = `<g id="${uid}-tip" opacity="0">
        <rect id="${uid}-bg" x="0" y="2" rx="4" ry="4" height="16" fill="rgba(0,0,0,0.85)" stroke="${color}" stroke-width="0.5"/>
        <text id="${uid}-txt" y="14" fill="#fff" font-size="9" font-family="JetBrains Mono, monospace" text-anchor="middle"></text>
    </g>`;

    return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">${bars}${tooltip}</svg>`;
}

function buildAreaChart(data, color, w, h) {
    if (!data || data.length < 2) {
        return `<svg width="${w}" height="${h}"><text x="${w / 2}" y="${h / 2}" text-anchor="middle" fill="rgba(255,255,255,0.2)" font-size="11" font-family="JetBrains Mono, monospace">No data yet</text></svg>`;
    }

    // Compute cumulative values
    const cumulative = [];
    let total = 0;
    for (const d of data) {
        total += d.value;
        cumulative.push(total);
    }

    const max = Math.max(...cumulative, 1);
    const pad = 8;

    const points = cumulative.map((v, i) => {
        const x = pad + (i / (cumulative.length - 1)) * (w - pad * 2);
        const y = pad + (1 - v / max) * (h - pad * 2);
        return `${x},${y}`;
    }).join(" ");

    const lastX = pad + ((cumulative.length - 1) / (cumulative.length - 1)) * (w - pad * 2);
    const gid = "g" + Math.random().toString(36).slice(2, 7);

    return `
        <svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
            <defs>
                <linearGradient id="${gid}" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stop-color="${color}" stop-opacity="0.25"/>
                    <stop offset="100%" stop-color="${color}" stop-opacity="0.02"/>
                </linearGradient>
            </defs>
            <polygon points="${pad},${h - pad} ${points} ${lastX},${h - pad}" fill="url(#${gid})"/>
            <polyline points="${points}" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>`;
}

function getGroupMax(daily, categories) {
    let max = 1;
    for (const cat of categories) {
        const data = daily[cat];
        if (data) {
            for (const d of data) {
                if (d.value > max) max = d.value;
            }
        }
    }
    return max;
}

function renderCharts(daily) {
    // Each group gets its own shared Y-axis scale
    const deathsMax = getGroupMax(daily, ["us_deaths", "iran_deaths", "other_deaths"]);
    const injuriesMax = getGroupMax(daily, ["us_injuries", "iran_injuries", "other_injuries"]);

    // Deaths charts
    setChart("chart-us-deaths", "us_deaths", daily, "bar", deathsMax);
    setChart("chart-iran-deaths", "iran_deaths", daily, "bar", deathsMax);
    setChart("chart-other-deaths", "other_deaths", daily, "bar", deathsMax);

    // Injuries charts
    setChart("chart-us-injuries", "us_injuries", daily, "bar", injuriesMax);
    setChart("chart-iran-injuries", "iran_injuries", daily, "bar", injuriesMax);
    setChart("chart-other-injuries", "other_injuries", daily, "bar", injuriesMax);

    // Displaced chart (own scale)
    setChart("chart-displaced", "displaced", daily, "area", null);
}

function setChart(containerId, category, daily, chartType, sharedMax) {
    const el = document.getElementById(containerId);
    if (el) el.innerHTML = renderChartCard(category, daily[category], chartType, sharedMax);
}

function renderChartCard(category, data, chartType, sharedMax) {
    const color = COLORS[category];
    const label = LABELS[category];
    const w = 480;
    const h = 180;

    let chart;
    if (chartType === "area") {
        chart = buildAreaChart(data, color, w, h);
    } else {
        chart = buildBarChart(data, color, w, h, sharedMax);
    }

    const startDate = data && data.length > 0 ? data[0].date : "Mar 1";
    const endDate = data && data.length > 0 ? data[data.length - 1].date : "Now";
    const count = data ? data.length : 0;

    return `
        <div class="chart-card">
            <div class="chart-card-label">${label} — Daily</div>
            <div class="chart-area">${chart}</div>
            <div class="chart-footer">
                <span>${startDate}</span>
                <span>${count} days</span>
                <span>${endDate}</span>
            </div>
        </div>`;
}

// --- Tooltip helpers (called from inline SVG event handlers) ---

function showBarTooltip(uid, idx, date, value, x, barW) {
    const tip = document.getElementById(uid + "-tip");
    const bg = document.getElementById(uid + "-bg");
    const txt = document.getElementById(uid + "-txt");
    if (!tip || !bg || !txt) return;

    const label = date + ": " + value.toLocaleString("en-US");
    txt.textContent = label;
    const textW = label.length * 5.5 + 12;
    const cx = x + barW / 2;
    bg.setAttribute("width", textW);
    bg.setAttribute("x", cx - textW / 2);
    txt.setAttribute("x", cx);
    tip.setAttribute("opacity", "1");
}

function hideBarTooltip(uid) {
    const tip = document.getElementById(uid + "-tip");
    if (tip) tip.setAttribute("opacity", "0");
}

function renderSources(sources) {
    // sources is now an object keyed by group: { deaths: [...], injuries: [...], displaced: [...] }
    if (!sources || typeof sources !== "object") return;

    for (const group of ["deaths", "injuries", "displaced"]) {
        const container = document.getElementById(`sources-${group}`);
        if (!container) continue;

        const items = sources[group] || [];
        if (items.length === 0) {
            container.innerHTML = '<div class="cas-loading" style="padding:1rem 0">No sources available yet</div>';
            continue;
        }

        container.innerHTML = "";
        const seen = new Set();
        for (const src of items) {
            const title = src.title || src.url;
            if (seen.has(title)) continue;
            seen.add(title);

            let domain = "";
            try {
                const hostname = new URL(src.url).hostname.replace("www.", "");
                if (hostname.includes("vertexaisearch.cloud.google.com") || hostname.includes("googleapis.com")) {
                    domain = title;
                } else {
                    domain = hostname;
                }
            } catch (e) {
                domain = title;
            }
            const item = document.createElement("div");
            item.className = "source-item";
            item.innerHTML = `
                <span class="source-domain">${domain}</span>
                <a href="${src.url}" target="_blank" rel="noopener">${title}</a>
            `;
            container.appendChild(item);
        }
    }
}

function toggleSources(group) {
    const container = document.getElementById(`sources-${group}`);
    const icon = document.getElementById(`sources-icon-${group}`);
    if (!container) return;

    const isCollapsed = container.classList.contains("collapsed");
    if (isCollapsed) {
        container.classList.remove("collapsed");
        container.style.maxHeight = container.scrollHeight + "px";
        container.style.opacity = "1";
        if (icon) icon.classList.add("open");
    } else {
        container.classList.add("collapsed");
        container.style.maxHeight = "0";
        container.style.opacity = "0";
        if (icon) icon.classList.remove("open");
    }
}

function renderDashboard(data) {
    // Hide loading
    const loading = document.getElementById("cas-loading");
    if (loading) loading.style.display = "none";

    renderSummaryCards(data.totals);
    renderCharts(data.daily);
    renderSources(data.sources);

    // Update timestamp
    const dateEl = document.getElementById("cas-date");
    if (dateEl) {
        const now = new Date();
        dateEl.textContent = now.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });
    }
}

// Initialize
document.addEventListener("DOMContentLoaded", async () => {
    try {
        const data = await fetchCasualties();
        renderDashboard(data);
    } catch (e) {
        console.error("Failed to load casualty data:", e);
        const loading = document.getElementById("cas-loading");
        if (loading) loading.textContent = "Failed to load data. Please refresh.";
    }

    // Refresh every 5 minutes
    setInterval(async () => {
        try {
            const data = await fetchCasualties();
            renderDashboard(data);
        } catch (e) {
            console.error("Failed to refresh casualty data:", e);
        }
    }, 300000);
});

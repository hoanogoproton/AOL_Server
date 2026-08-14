let cyclesData = [];
let selectedCycleId = null;
let refreshInterval = null;
let clientUrl = '';
let liveEventId = null;
let liveImageTimer = null;

document.addEventListener('DOMContentLoaded', () => {
    loadCycles();
    checkHealth();
    pollLive();
    refreshInterval = setInterval(() => {
        checkHealth();
        loadCycles(false);
    }, 5000);
    liveImageTimer = setInterval(pollLive, 2000);
});

async function fetchJSON(url) {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return resp.json();
}

async function checkHealth() {
    try {
        const data = await fetchJSON('/health');
        const dot = document.getElementById('serverStatus');
        const text = document.getElementById('serverStatusText');
        dot.className = 'status-dot online';
        text.textContent = 'Online';
        document.getElementById('queueSize').textContent = data.queue_size ?? '-';
        document.getElementById('modelPath').textContent = data.model_path ? data.model_path.split('/').pop().split('\\').pop() : '-';
        if (data.client_url) clientUrl = data.client_url;
    } catch {
        const dot = document.getElementById('serverStatus');
        const text = document.getElementById('serverStatusText');
        dot.className = 'status-dot offline';
        text.textContent = 'Offline';
    }
}

async function pollLive() {
    try {
        const data = await fetchJSON('/api/v1/live');
        renderLive(data.image);
    } catch {
        const pulse = document.getElementById('livePulse');
        const sub = document.getElementById('liveSub');
        pulse.className = 'live-pulse idle';
        sub.textContent = 'Server offline';
        setLiveResult('pending', '--');
    }
}

function renderLive(image) {
    const pulse = document.getElementById('livePulse');
    const sub = document.getElementById('liveSub');
    const placeholder = document.getElementById('livePlaceholder');
    const imgEl = document.getElementById('liveImage');
    const overlay = document.getElementById('liveOverlay');
    const livePanel = document.getElementById('livePanel');

    if (!image) {
        pulse.className = 'live-pulse idle';
        sub.textContent = 'Waiting...';
        imgEl.style.display = 'none';
        placeholder.style.display = 'flex';
        overlay.style.display = 'none';
        setLiveResult('pending', '--');
        document.getElementById('liveInfo').innerHTML = '';
        liveEventId = null;
        return;
    }

    placeholder.style.display = 'none';
    overlay.style.display = 'flex';
    pulse.className = 'live-pulse';

    const isNew = image.event_id !== liveEventId;
    liveEventId = image.event_id;

    if (isNew) {
        const src = `/api/v1/images/${image.event_id}/annotated?t=${Date.now()}`;
        if (imgEl.src !== src) {
            imgEl.src = src;
            imgEl.style.display = 'block';
        }
        imgEl.classList.remove('updated');
        void imgEl.offsetWidth;
        imgEl.classList.add('updated');
        livePanel.classList.add('fresh');
        setTimeout(() => livePanel.classList.remove('fresh'), 700);
    }

    const result = image.result || null;
    const processed = image.processed_at || image.received_at || '';
    sub.textContent = processed ? processed.replace('T', ' ').substring(11, 19) : '';

    const step = image.step || 0;
    const passed = result?.evaluation?.passed;

    document.getElementById('ovCycle').textContent = `Cycle ${image.cycle_id}`;
    document.getElementById('ovStep').textContent = `Step ${step}`;
    document.getElementById('ovSide').textContent = `Side ${image.camera_side || '-'}`;

    const stepLabel = step === 1 ? 'PASS' : 'OK';
    if (image.image_status === 'ERROR') {
        setLiveResult('ng', 'ERR');
    } else if (passed === true) {
        setLiveResult('ok', stepLabel);
    } else if (passed === false) {
        setLiveResult('ng', step === 1 ? 'FAIL' : 'NG');
    } else {
        setLiveResult('pending', '--');
    }

    renderLiveInfo(image, result);
}

function setLiveResult(cls, text) {
    const chip = document.getElementById('liveResult');
    chip.className = `live-result ${cls}`;
    document.getElementById('liveResultValue').textContent = text;
}

function renderLiveInfo(image, result) {
    const info = document.getElementById('liveInfo');
    const detections = result?.detections || [];
    const evaluation = result?.evaluation || null;
    const errorCode = result?.error_code || image.error_code || '';

    const detRows = detections.length
        ? detections.map((d, i) => `
            <div class="det-row">
                <span class="det-idx">#${i + 1}</span>
                <span class="det-cls">class=${d.class_id}</span>
                <span class="det-cfg">${d.confidence.toFixed(3)}</span>
            </div>
        `).join('')
        : '<div class="info-empty">No detections</div>';

    const roiItems = (evaluation?.roi_results || []).map(r => `
        <div class="roi-item ${r.passed ? 'pass' : 'fail'}">
            <span class="roi-name">${r.roi_id}</span>
            <span class="roi-count">${r.class_name} | count=${r.count} [${r.min_count}-${r.max_count}]</span>
            <span class="roi-verdict">${r.passed ? 'PASS' : 'FAIL'}</span>
        </div>
    `).join('') || '<div class="info-empty">No ROI data</div>';

    info.innerHTML = `
        <div class="info-section">
            <h3>Detections (${detections.length})</h3>
            <div class="live-detections">${detRows}</div>
        </div>
        <div class="info-section">
            <h3>ROI Evaluation</h3>
            <div class="roi-list">${roiItems}</div>
        </div>
        <div class="info-section">
            <h3>Error</h3>
            <div class="info-error">${errorCode && errorCode !== 'NONE' ? errorCode : 'NONE'}</div>
        </div>
    `;
}

async function loadCycles(showLoader = true) {
    try {
        const data = await fetchJSON('/api/v1/cycles?limit=5');
        cyclesData = data.cycles || [];
        renderCycleList(cyclesData);
        document.getElementById('cycleCount').textContent = cyclesData.length;
        if (selectedCycleId) {
            const stillExists = cyclesData.find(c => c.cycle_id === selectedCycleId);
            if (!stillExists) {
                selectedCycleId = null;
                document.getElementById('detailView').style.display = 'none';
                document.getElementById('emptyState').style.display = 'flex';
            }
        }
    } catch (err) {
        console.error('Failed to load cycles:', err);
    }
}

function filterCycles() {
    const query = document.getElementById('cycleSearch').value.toLowerCase();
    const filtered = query
        ? cyclesData.filter(c => c.cycle_id.toLowerCase().includes(query))
        : cyclesData;
    renderCycleList(filtered);
}

function renderCycleList(cycles) {
    const container = document.getElementById('cycleList');
    if (!cycles.length) {
        container.innerHTML = '<div class="empty-state">No cycles found</div>';
        return;
    }
    container.innerHTML = cycles.map(c => {
        const finalTag = getFinalTag(c);
        const active = c.cycle_id === selectedCycleId ? 'active' : '';
        const created = c.created_at ? c.created_at.replace('T', ' ').substring(0, 19) : '-';
        return `
            <div class="cycle-card ${active}" onclick="selectCycle('${c.cycle_id}')">
                <div class="cycle-id">${c.cycle_id}</div>
                <div class="cycle-meta">
                    <span>${c.camera_side || '-'}</span>
                    <span>${c.tube_type || '-'}</span>
                    ${finalTag}
                    <span>${created}</span>
                </div>
            </div>
        `;
    }).join('');
}

function getFinalTag(c) {
    if (!c.final_result) return '<span class="tag tag-pending">Pending</span>';
    if (c.final_result === 'OK') return '<span class="tag tag-ok">OK</span>';
    if (c.com_status === 'ACK') return '<span class="tag tag-ack">ACK</span>';
    return '<span class="tag tag-ng">NG</span>';
}

async function selectCycle(cycleId) {
    selectedCycleId = cycleId;
    document.getElementById('emptyState').style.display = 'none';
    document.getElementById('detailView').style.display = 'block';

    renderCycleList(cyclesData);

    try {
        const data = await fetchJSON(`/api/v1/cycles/${cycleId}/images`);
        renderCycleDetail(data.cycle, data.images);
    } catch (err) {
        console.error('Failed to load cycle detail:', err);
        document.getElementById('detailCycleId').textContent = cycleId;
        document.getElementById('imagesContainer').innerHTML = '<div class="empty-state">Failed to load details</div>';
    }
}

function renderCycleDetail(cycle, images) {
    document.getElementById('detailCycleId').textContent = cycle.cycle_id;
    document.getElementById('detailSide').textContent = `Side: ${cycle.camera_side || '-'}`;
    document.getElementById('detailTubeType').textContent = `Type: ${cycle.tube_type || '-'}`;
    document.getElementById('detailCreated').textContent = cycle.created_at ? cycle.created_at.replace('T', ' ').substring(0, 19) : '-';

    renderStepChip('chipStep1', 'step1Status', cycle.step1_status, 'PASS', 'FAIL');
    renderStepChip('chipStep3', 'step3Status', cycle.step3_status, 'OK', 'NG');

    const finalChip = document.getElementById('chipFinal');
    const finalVal = document.getElementById('finalResult');
    if (cycle.final_result === 'OK') {
        finalChip.className = 'result-chip chip-final ok';
        finalVal.textContent = 'OK';
    } else if (cycle.final_result === 'NG') {
        finalChip.className = 'result-chip chip-final ng';
        finalVal.textContent = 'NG';
    } else {
        finalChip.className = 'result-chip chip-final pending';
        finalVal.textContent = '--';
    }

    const errorDiv = document.getElementById('detailError');
    if (cycle.final_error && cycle.final_error !== 'NONE') {
        errorDiv.style.display = 'flex';
        document.getElementById('errorText').textContent = cycle.final_error;
    } else {
        errorDiv.style.display = 'none';
    }

    const container = document.getElementById('imagesContainer');
    if (!images.length) {
        container.innerHTML = '<div class="empty-state">No images in this cycle</div>';
        return;
    }

    container.innerHTML = images.map(img => {
        const step = img.step || 0;
        const stepClass = step === 1 ? 'step-1' : step === 3 ? 'step-3' : '';
        const status = (img.image_status || '').toLowerCase();
        const statusClass = status === 'done' ? 'done' : status === 'error' ? 'error' : 'processing';

        const result = img.result || null;
        const detections = result?.detections || [];
        const evaluation = result?.evaluation || null;
        const errorCode = result?.error_code || img.error_code || '';

        const hasAnnotated = img.annotated_path != null;

        return `
            <div class="image-card">
                <div class="image-card-header">
                    <div class="left">
                        <span class="step-badge ${stepClass}">Step ${step}</span>
                        <span class="event-id">${img.event_id}</span>
                    </div>
                    <span class="status-badge ${statusClass}">${img.image_status}</span>
                </div>
                <div class="image-card-body">
                    <div class="image-preview">
                        <img src="/api/v1/images/${img.event_id}/annotated" alt="Annotated ${img.event_id}"
                             onclick="toggleZoom(this)"
                             loading="lazy">
                    </div>
                    <div class="image-info">
                        ${renderDetectionSection(detections)}
                        ${renderRoiSection(evaluation)}
                        ${renderErrorSection(errorCode)}
                        ${renderImageMetaSection(img)}
                    </div>
                </div>
            </div>
        `;
    }).join('');
}

function renderStepChip(chipId, valId, status, passVal, failVal) {
    const chip = document.getElementById(chipId);
    const val = document.getElementById(valId);
    if (!status) {
        chip.className = 'result-chip pending';
        val.textContent = '--';
    } else if (status === passVal) {
        chip.className = 'result-chip pass';
        val.textContent = status;
    } else {
        chip.className = 'result-chip fail';
        val.textContent = status;
    }
}

function renderDetectionSection(detections) {
    if (!detections || !detections.length) {
        return `
            <div class="info-section">
                <h3>Detections</h3>
                <div style="color:var(--text-muted);font-family:var(--font-mono);font-size:0.7rem;">No detections</div>
            </div>
        `;
    }
    const rows = detections.map(d => `
        <tr>
            <td class="det-class">${d.class_id}</td>
            <td class="det-conf">${d.confidence.toFixed(4)}</td>
            <td>(${d.x1.toFixed(1)}, ${d.y1.toFixed(1)})</td>
            <td>(${d.x2.toFixed(1)}, ${d.y2.toFixed(1)})</td>
        </tr>
    `).join('');
    return `
        <div class="info-section">
            <h3>Detections (${detections.length})</h3>
            <table class="det-table">
                <thead>
                    <tr>
                        <th>Class</th>
                        <th>Conf</th>
                        <th>Top-Left</th>
                        <th>Bottom-Right</th>
                    </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
    `;
}

function renderRoiSection(evaluation) {
    if (!evaluation || !evaluation.roi_results || !evaluation.roi_results.length) {
        return `
            <div class="info-section">
                <h3>ROI Evaluation</h3>
                <div style="color:var(--text-muted);font-family:var(--font-mono);font-size:0.7rem;">No ROI data</div>
            </div>
        `;
    }
    const items = evaluation.roi_results.map(r => `
        <div class="roi-item ${r.passed ? 'pass' : 'fail'}">
            <span class="roi-name">${r.roi_id}</span>
            <span class="roi-count">${r.class_name} | count=${r.count} [${r.min_count}-${r.max_count}]</span>
            <span class="roi-verdict">${r.passed ? 'PASS' : 'FAIL'}</span>
        </div>
    `).join('');
    return `
        <div class="info-section">
            <h3>ROI Evaluation</h3>
            <div class="roi-list">${items}</div>
        </div>
    `;
}

function renderErrorSection(errorCode) {
    if (!errorCode || errorCode === 'NONE') return '';
    return `
        <div class="info-section">
            <h3>Error Code</h3>
            <div class="info-error">${errorCode}</div>
        </div>
    `;
}

function renderImageMetaSection(img) {
    return `
        <div class="info-section">
            <h3>Metadata</h3>
            <table class="det-table">
                <tbody>
                    <tr><td style="color:var(--text-muted)">Event ID</td><td style="color:var(--text-secondary)">${img.event_id}</td></tr>
                    <tr><td style="color:var(--text-muted)">Step</td><td style="color:var(--text-secondary)">${img.step}</td></tr>
                    <tr><td style="color:var(--text-muted)">Side</td><td style="color:var(--text-secondary)">${img.camera_side || '-'}</td></tr>
                    <tr><td style="color:var(--text-muted)">Type</td><td style="color:var(--text-secondary)">${img.tube_type || '-'}</td></tr>
                    <tr><td style="color:var(--text-muted)">Captured</td><td style="color:var(--text-secondary)">${img.capture_timestamp ? img.capture_timestamp.replace('T', ' ').substring(0, 19) : '-'}</td></tr>
                    <tr><td style="color:var(--text-muted)">Processed</td><td style="color:var(--text-secondary)">${img.processed_at ? img.processed_at.replace('T', ' ').substring(0, 19) : '-'}</td></tr>
                    <tr><td style="color:var(--text-muted)">SHA256</td><td style="color:var(--text-secondary);font-size:0.6rem">${img.image_sha256 ? img.image_sha256.substring(0, 16) + '...' : '-'}</td></tr>
                </tbody>
            </table>
        </div>
    `;
}

function toggleZoom(img) {
    img.classList.toggle('zoomed');
}

function refreshAll() {
    checkHealth();
    loadCycles();
    if (selectedCycleId) {
        selectCycle(selectedCycleId);
    }
}

async function sendReset() {
    if (!confirm('Send reset signal for Side L and R?')) return;
    const btn = event.target;
    btn.disabled = true;
    btn.textContent = 'Resetting...';
    const results = await Promise.allSettled(
        ['L', 'R'].map(side =>
            fetch(`${clientUrl}/api/v1/reset/${side}`, { method: 'POST' })
        )
    );
    const allOk = results.every(r => r.status === 'fulfilled' && r.value.ok);
    btn.textContent = allOk ? 'Reset ✓' : 'Reset ✗';
    setTimeout(() => { btn.textContent = 'Reset'; btn.disabled = false; }, 2000);
    setTimeout(() => refreshAll(), 500);
}
// Gera o PPT de Resultados Comerciais com dados frescos do cache Ploomes.
// Chamado por /api/ploomes/forecast-ppt no server.js — retorna Buffer pra download.
const pptxgen = require('pptxgenjs');

const FIELD_MRR = 'deal_1F7F1DEC-39B3-4621-9237-96D7793DAD03';
const FIELD_SETUP = 'deal_90CB9147-95C6-4A5F-8607-A2B5225ADFC3';
const FIELD_PROPOSAL_DATE = 'deal_12C64ECD-CD5C-4C83-B0CD-7E7CCB415D7E';
const FIELD_FORECAST = 'deal_7F644269-46FE-4486-AD12-BEFA9C7E27BC';

const C = {
    bg: '0B0F19', surface: '141B2E', surfaceLite: '1E2740',
    text: 'FFFFFF', muted: '94A3B8', mutedSoft: 'CBD5E1',
    roxo: '8B5CF6', roxoSoft: 'C4B5FD',
    verde: '22C55E', verdeSoft: '86EFAC',
    ambar: 'F59E0B', vermelho: 'EF4444',
    ciano: '06B6D4', lima: '84CC16', rosa: 'EC4899'
};

const brl = (n) => 'R$ ' + Number(n || 0).toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
const brlk = (n) => {
    n = Number(n || 0);
    if (n >= 1000) return 'R$ ' + (n/1000).toFixed(n >= 10000 ? 0 : 1).replace('.', ',') + 'k';
    return brl(n);
};
const norm = (s) => String(s || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '');
const getProp = (d, key) => (d.OtherProperties || []).find(x => x.FieldKey === key);
const getMRR = (d) => getProp(d, FIELD_MRR)?.DecimalValue || 0;
const getSetup = (d) => getProp(d, FIELD_SETUP)?.DecimalValue || 0;
const getForecast = (d) => getProp(d, FIELD_FORECAST)?.ObjectValueName || null;
const getProposalDate = (d) => {
    const p = getProp(d, FIELD_PROPOSAL_DATE);
    return p?.DateTimeValue ? new Date(p.DateTimeValue) : null;
};
const hasProposalMarker = (d) => {
    const props = d.OtherProperties || [];
    for (const p of props) {
        const val = String(p.ObjectValueName || p.StringValue || '').toLowerCase().trim();
        if (val.includes('proposta gerada') || val.includes('proposta enviada')) return true;
    }
    return false;
};
const isSalesPipeline = (d) => ['Funil de Vendas', 'Sankhya', 'Farmer', 'Farmer IPCA'].includes(d.Pipeline?.Name || '');
const getPipelineName = (d) => d.Pipeline?.Name || '';

const FORECAST_MAP = {
    'Semana 1': 'S1', 'Semana 2': 'S2', 'Semana 3': 'S3', 'Semana 4': 'S4',
    'Pessimista': 'S1', 'Realista': 'S2', 'Otimista': 'S3'
};

function slideHeader(s, sideColor, eyebrow, title, subtitle) {
    s.background = { color: C.bg };
    s.addShape('rect', { x: 0, y: 0, w: 0.3, h: 7.5, fill: { color: sideColor } });
    s.addText(eyebrow, { x: 0.7, y: 0.4, w: 12, h: 0.4, fontFace: 'Arial', fontSize: 11, bold: true, color: sideColor, charSpacing: 6 });
    s.addText(title, { x: 0.7, y: 0.75, w: 12, h: 0.7, fontFace: 'Arial', fontSize: 26, bold: true, color: C.text });
    if (subtitle) s.addText(subtitle, { x: 0.7, y: 1.45, w: 12, h: 0.35, fontFace: 'Arial', fontSize: 13, color: C.muted });
}
function pageNum(s, n, total) {
    s.addText(String(n).padStart(2,'0') + ' / ' + String(total).padStart(2,'0'), {
        x: 12.3, y: 7.2, w: 0.8, h: 0.3, fontFace: 'Arial', fontSize: 11, color: C.muted, align: 'right'
    });
}

async function generateForecastPpt(caches, options = {}) {
    const won = caches.won || [];
    const lost = caches.lost || [];
    const open = caches.open || [];
    const forecast = caches.forecast || [];
    const meetings = caches.meetings || [];
    const usersArr = caches.users || [];
    const userMap = {};
    usersArr.forEach(u => { userMap[u.Id] = u.Name; });

    // Deduplica todos os deals (open + forecast + won + lost) por Id
    const dealsMap = new Map();
    [...forecast, ...open, ...won, ...lost].forEach(d => {
        if (d.Id && !dealsMap.has(d.Id)) dealsMap.set(d.Id, d);
    });
    const allDeals = [...dealsMap.values()];

    // === Período: mês corrente ===
    const now = new Date();
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
    const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59);
    const sinceReunioes = options.sinceReunioes ? new Date(options.sinceReunioes) : new Date(now.getTime() - 7 * 86400000);

    const inMonth = (dt) => {
        if (!dt) return false;
        const t = new Date(dt).getTime();
        return t >= monthStart.getTime() && t <= monthEnd.getTime();
    };

    // === GANHOS DO MÊS (deal ganho com FinishDate no mês) ===
    const wonThisMonth = won.filter(d => isSalesPipeline(d) && getPipelineName(d) !== 'Farmer IPCA' && inMonth(d.FinishDate));
    const ganhosMrr = wonThisMonth.reduce((s, d) => s + getMRR(d), 0);
    const ganhosSetup = wonThisMonth.reduce((s, d) => s + getSetup(d), 0);

    // === FORECAST GARANTIDO: deals abertos com Forecast preenchido ===
    const garantidoDeals = allDeals.filter(d => {
        if (d.StatusId === 3) return false; // não perdidos
        if (d.StatusId === 2) return false; // não ganhos (esses viraram Ganho, não Garantido)
        if (!isSalesPipeline(d)) return false;
        const fc = getForecast(d);
        return fc && FORECAST_MAP[fc];
    }).sort((a, b) => getMRR(b) - getMRR(a));
    const garantidoMrr = garantidoDeals.reduce((s, d) => s + getMRR(d), 0);
    const garantidoSetup = garantidoDeals.reduce((s, d) => s + getSetup(d), 0);

    // === PIPELINE (chance de entrar): deals abertos SEM Forecast em stage avançado ===
    const stageAvancado = /reconhec|considera|proposta|aprovad|formalizac|negociac/i;
    const pipelineDeals = allDeals.filter(d => {
        if (d.StatusId !== 1) return false; // só abertos
        if (!isSalesPipeline(d)) return false;
        const fc = getForecast(d);
        if (fc && FORECAST_MAP[fc]) return false; // já é garantido
        const stageName = d.Stage?.Name || '';
        if (!stageAvancado.test(stageName)) return false;
        return true;
    }).sort((a, b) => getMRR(b) - getMRR(a));
    const pipelineMrr = pipelineDeals.reduce((s, d) => s + getMRR(d), 0);
    const pipelineSetup = pipelineDeals.reduce((s, d) => s + getSetup(d), 0);

    // === REUNIÕES REALIZADAS por vendedor no período (desde sinceReunioes) ===
    const reunioesByCreator = {};
    for (const t of meetings) {
        if (!norm(t.Title).includes('reuniao realizada')) continue;
        if (t.Finished !== true) continue;
        const dtStr = t.FinishDate || t.DateTime;
        if (!dtStr) continue;
        const dt = new Date(dtStr);
        if (dt < sinceReunioes || dt > now) continue;
        const creator = userMap[t.CreatorId] || t.Creator?.Name || '(sem creator)';
        reunioesByCreator[creator] = (reunioesByCreator[creator] || 0) + 1;
    }
    const reunioesSort = Object.entries(reunioesByCreator).sort((a, b) => b[1] - a[1]);
    const totalReunioes = reunioesSort.reduce((s, [_, c]) => s + c, 0);

    // === PROPOSTAS ENVIADAS por owner no período (mesma regra do sec7Inter) ===
    const propostasByOwner = {};
    for (const d of allDeals) {
        // (a) Data proposta no periodo
        const propDate = getProposalDate(d);
        const inRangeByProp = propDate && propDate >= sinceReunioes && propDate <= now;
        // (b) MRR/Setup preenchido + CreateDate no periodo
        const hasValue = getMRR(d) > 0 || getSetup(d) > 0;
        const createDate = d.CreateDate ? new Date(d.CreateDate) : null;
        const createdInRange = createDate && createDate >= sinceReunioes && createDate <= now;
        // (c) marcador + LastUpdate no periodo
        const lastUpdate = d.LastUpdateDate ? new Date(d.LastUpdateDate) : null;
        const markerInRange = hasProposalMarker(d) && lastUpdate && lastUpdate >= sinceReunioes && lastUpdate <= now;
        if (!inRangeByProp && !(hasValue && createdInRange) && !markerInRange) continue;
        const owner = d.Owner?.Name || '(sem owner)';
        propostasByOwner[owner] = (propostasByOwner[owner] || 0) + 1;
    }
    const propostasSort = Object.entries(propostasByOwner).sort((a, b) => b[1] - a[1]);
    const totalPropostas = propostasSort.reduce((s, [_, c]) => s + c, 0);

    // === Meta consolidada ===
    const META_MRR = 200000;
    const totalConfirmado = ganhosMrr + garantidoMrr;
    const coberturaGarantida = Math.round((totalConfirmado / META_MRR) * 100);
    const coberturaTotal = Math.round(((totalConfirmado + pipelineMrr) / META_MRR) * 100);
    const gapPraMeta = Math.max(0, META_MRR - totalConfirmado);

    // Formata datas
    const fmtDt = (d) => `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}/${d.getFullYear()}`;
    const cortesLabel = `Corte ${fmtDt(now)}`;
    const sinceLabel = fmtDt(sinceReunioes);
    const untilLabel = fmtDt(now);

    const pres = new pptxgen();
    pres.layout = 'LAYOUT_WIDE';
    const TOTAL_SLIDES = 11;

    // ==== SLIDE 1: CAPA ====
    {
        const s = pres.addSlide();
        s.background = { color: C.bg };
        s.addShape('rect', { x: 0, y: 0, w: 0.3, h: 7.5, fill: { color: C.verde } });
        s.addText('LINCROS · COMERCIAL', { x: 0.7, y: 0.6, w: 12, h: 0.4, fontFace: 'Arial', fontSize: 11, bold: true, color: C.muted, charSpacing: 8 });
        s.addText('Resultados Comerciais', { x: 0.7, y: 1.6, w: 12, h: 1.0, fontFace: 'Arial', fontSize: 52, bold: true, color: C.text });
        s.addText('A semana em que o time destravou o mês', { x: 0.7, y: 2.7, w: 12, h: 0.6, fontFace: 'Arial', fontSize: 22, color: C.verdeSoft });
        s.addText(`${cortesLabel} · dados frescos do Ploomes`, { x: 0.7, y: 3.35, w: 12, h: 0.4, fontFace: 'Arial', fontSize: 14, color: C.muted });

        s.addShape('rect', { x: 0.7, y: 4.15, w: 12, h: 2.7, fill: { color: C.surface }, line: { color: C.verde, width: 1 } });
        s.addText(`${coberturaGarantida}% DA META JÁ GARANTIDA`, { x: 1.0, y: 4.35, w: 11, h: 0.4, fontFace: 'Arial', fontSize: 12, color: C.verde, charSpacing: 4, bold: true });
        s.addText(`${brlk(ganhosMrr)} ganhos + ${brlk(garantidoMrr)} no forecast = ${brlk(totalConfirmado)} / ${brlk(META_MRR)}`, {
            x: 1.0, y: 4.8, w: 11, h: 0.7, fontFace: 'Arial', fontSize: 22, bold: true, color: C.text
        });
        s.addText(`Pipeline (chance de entrar): ${brlk(pipelineMrr)} MRR · ${pipelineDeals.length} OPOs aquecendo`, {
            x: 1.0, y: 5.6, w: 11, h: 0.4, fontFace: 'Arial', fontSize: 15, color: C.verdeSoft
        });
        s.addText(`Gap pra fechar meta: ${brlk(gapPraMeta)} · cobertura total com pipeline: ${coberturaTotal}%`, {
            x: 1.0, y: 6.05, w: 11, h: 0.4, fontFace: 'Arial', fontSize: 13, color: C.muted
        });
        pageNum(s, 1, TOTAL_SLIDES);
    }

    // ==== SLIDE 2: EVOLUÇÃO ====
    {
        const s = pres.addSlide();
        slideHeader(s, C.verde, 'EVOLUÇÃO', 'Onde estávamos vs onde estamos', 'Comparação vs corte de 17/07 (dados frescos do Ploomes)');
        const cy = 2.2, ch = 4.5;
        const ax = 0.7, aw = 5.9;
        s.addShape('rect', { x: ax, y: cy, w: aw, h: ch, fill: { color: C.surface } });
        s.addText('CORTE 17/07 · SEMANA 2', { x: ax + 0.3, y: cy + 0.25, w: aw - 0.6, h: 0.4, fontFace: 'Arial', fontSize: 11, bold: true, color: C.muted, charSpacing: 4 });
        s.addText('Forecast oficial (S2)', { x: ax + 0.3, y: cy + 0.85, w: aw - 0.6, h: 0.3, fontFace: 'Arial', fontSize: 11, color: C.muted });
        s.addText('R$ 5,3k', { x: ax + 0.3, y: cy + 1.2, w: aw - 0.6, h: 1.0, fontFace: 'Arial', fontSize: 44, bold: true, color: C.muted });
        s.addText('MRR dentro do funil oficial', { x: ax + 0.3, y: cy + 2.2, w: aw - 0.6, h: 0.35, fontFace: 'Arial', fontSize: 11, color: C.muted });
        s.addShape('line', { x: ax + 0.3, y: cy + 2.7, w: aw - 0.6, h: 0, line: { color: C.muted, width: 1, transparency: 70 } });
        s.addText('R$ 155k+ em contas quentes fora do forecast', { x: ax + 0.3, y: cy + 2.9, w: aw - 0.6, h: 0.4, fontFace: 'Arial', fontSize: 13, color: C.muted });
        s.addText('R$ 21,3k ganhos até 17/07 (10,7% da meta)', { x: ax + 0.3, y: cy + 3.4, w: aw - 0.6, h: 0.4, fontFace: 'Arial', fontSize: 13, color: C.muted });
        s.addText('Reuniões: 68/197 (34,5%) · Propostas: 22/101,5', { x: ax + 0.3, y: cy + 3.9, w: aw - 0.6, h: 0.35, fontFace: 'Arial', fontSize: 12, color: C.muted });

        const bx = 6.9, bw = 5.9;
        s.addShape('rect', { x: bx, y: cy, w: bw, h: ch, fill: { color: C.surface }, line: { color: C.verde, width: 2 } });
        s.addText(cortesLabel.toUpperCase(), { x: bx + 0.3, y: cy + 0.25, w: bw - 0.6, h: 0.4, fontFace: 'Arial', fontSize: 11, bold: true, color: C.verde, charSpacing: 4 });
        s.addText('Forecast oficial (Garantido)', { x: bx + 0.3, y: cy + 0.85, w: bw - 0.6, h: 0.3, fontFace: 'Arial', fontSize: 11, color: C.muted });
        s.addText(brlk(garantidoMrr), { x: bx + 0.3, y: cy + 1.2, w: bw - 0.6, h: 1.0, fontFace: 'Arial', fontSize: 44, bold: true, color: C.verde });
        s.addText(`MRR confirmado · ${garantidoDeals.length} OPOs mapeadas`, { x: bx + 0.3, y: cy + 2.2, w: bw - 0.6, h: 0.35, fontFace: 'Arial', fontSize: 11, color: C.verdeSoft });
        s.addShape('line', { x: bx + 0.3, y: cy + 2.7, w: bw - 0.6, h: 0, line: { color: C.verde, width: 1, transparency: 70 } });
        s.addText(`${brlk(pipelineMrr)} em pipeline aquecendo · ${pipelineDeals.length} OPOs`, { x: bx + 0.3, y: cy + 2.9, w: bw - 0.6, h: 0.4, fontFace: 'Arial', fontSize: 13, color: C.text });
        s.addText(`${coberturaGarantida}% da meta de ${brlk(META_MRR)} já garantida`, { x: bx + 0.3, y: cy + 3.4, w: bw - 0.6, h: 0.4, fontFace: 'Arial', fontSize: 13, bold: true, color: C.verde });
        s.addText(`Meta a bater até fim do mês: ${brlk(gapPraMeta)}`, { x: bx + 0.3, y: cy + 3.9, w: bw - 0.6, h: 0.35, fontFace: 'Arial', fontSize: 12, color: C.text });

        s.addShape('rect', { x: 0.7, y: 6.9, w: 12.1, h: 0.4, fill: { color: C.verde, transparency: 85 } });
        s.addText('Forecast oficial cresceu significativamente em 7 dias. Time saiu do gap e entrou na zona de execução.', {
            x: 0.9, y: 6.9, w: 11.7, h: 0.4, fontFace: 'Arial', fontSize: 13, italic: true, bold: true, color: C.verde, valign: 'middle'
        });
        pageNum(s, 2, TOTAL_SLIDES);
    }

    // ==== SLIDE 3: O QUE FUNCIONOU ====
    {
        const s = pres.addSlide();
        slideHeader(s, C.verde, 'O QUE FUNCIONOU', 'Movimentos que destravaram a semana', 'Execução do plano de 17/07 gerou resultado mensurável');
        const wins = [
            { icon: '✓', color: C.verde, title: '8 de 15 contas quentes migraram pro forecast em 7 dias', body: 'Taxa de 53% de conta quente → pipeline oficial. Alan puxou 2/3 da carteira (Inflow, EAU). Matheus puxou 3/3 (Cofermeta, Ponto Duplo). Lechuk trouxe Hoya e Artvinco.' },
            { icon: '✓', color: C.lima, title: `Forecast oficial cresceu para ${brlk(garantidoMrr)}`, body: `Time destravou ${garantidoDeals.length} OPOs com valores confirmados no Ploomes. Cobertura garantida da meta subiu para ${coberturaGarantida}% sem depender do pipeline em conversão.` },
            { icon: '✓', color: C.ciano, title: 'Ações do plano da semana passada rodaram', body: 'Webinar Sankhya gerou 15 agendas (Thiago/Cláudia/Maja/Agnes). Atuação em Losts foi distribuída pra cada executivo. Plano Laisa na Base identificou R$ 268k potencial de TMS.' },
            { icon: '✓', color: C.rosa, title: 'Pipeline pronto pra fechar o mês', body: `${brlk(pipelineMrr)} em ${pipelineDeals.length} OPOs aquecendo. Time construiu opcionalidade real pra Semana 4.` }
        ];
        wins.forEach((w, i) => {
            const y = 2.15 + i * 1.15;
            s.addShape('rect', { x: 0.7, y, w: 12.1, h: 1.0, fill: { color: C.surface }, line: { color: w.color, width: 1, transparency: 70 } });
            s.addShape('rect', { x: 0.7, y, w: 0.15, h: 1.0, fill: { color: w.color } });
            s.addText(w.icon, { x: 1.0, y: y + 0.2, w: 0.6, h: 0.6, fontFace: 'Arial', fontSize: 26, bold: true, color: w.color });
            s.addText(w.title, { x: 1.65, y: y + 0.12, w: 11.0, h: 0.4, fontFace: 'Arial', fontSize: 15, bold: true, color: C.text });
            s.addText(w.body, { x: 1.65, y: y + 0.5, w: 11.0, h: 0.5, fontFace: 'Arial', fontSize: 11.5, color: C.mutedSoft });
        });
        pageNum(s, 3, TOTAL_SLIDES);
    }

    // ==== SLIDE 4: INICIATIVAS EM ANDAMENTO ====
    {
        const s = pres.addSlide();
        slideHeader(s, C.roxo, 'INICIATIVAS EM ANDAMENTO', 'O que está sendo construído pra sustentar o resultado', 'Estruturas novas que entram em rotina até fim de julho');
        const items = [
            { n: '01', color: C.rosa, title: 'Máquina de geração de demanda reformulada', body: 'Novas campanhas no ar — incluindo LAISA pra Novos Negócios. Marketing (Fernanda + Iago) executando repositioning do topo do funil pra abastecer S1 de agosto.' },
            { n: '02', color: C.ciano, title: 'Pré-vendas redividido por frente de atuação', body: 'SDRs realocados por especialização: Novos Negócios, Farmer e Sankhya com times dedicados. Cada frente ganha foco e ritual próprio de qualificação.' },
            { n: '03', color: C.ambar, title: 'Revisão de investimento em mídia paga', body: 'Auditoria de gasto por plataforma (Meta, LinkedIn, Google) pra parar de queimar em campanhas sem retorno. ROAS real cruzado com Ploomes já visível no dashboard.' },
            { n: '04', color: C.verde, title: 'Ferramenta Leads Per Hour · entra quarta', body: 'Automatiza todo o processo de follow-up: cada lead recebe cadência sem depender de memória do SDR. Impacto direto: não perde touch, não deixa lead esfriar, converte mais SAL em SQL.' }
        ];
        items.forEach((it, i) => {
            const col = i % 2;
            const row = Math.floor(i / 2);
            const x = 0.7 + col * 6.2;
            const y = 2.15 + row * 2.4;
            const w = 5.9, h = 2.15;
            s.addShape('rect', { x, y, w, h, fill: { color: C.surface }, line: { color: it.color, width: 1, transparency: 65 } });
            s.addShape('rect', { x, y, w: 0.15, h, fill: { color: it.color } });
            s.addText(it.n, { x: x + 0.35, y: y + 0.15, w: 0.8, h: 0.5, fontFace: 'Arial', fontSize: 20, bold: true, color: it.color });
            s.addText(it.title, { x: x + 1.2, y: y + 0.15, w: w - 1.4, h: 0.65, fontFace: 'Arial', fontSize: 14, bold: true, color: C.text });
            s.addText(it.body, { x: x + 0.35, y: y + 0.9, w: w - 0.55, h: h - 1.0, fontFace: 'Arial', fontSize: 11, color: C.mutedSoft });
        });
        s.addText('Além das ações da semana passada — Webinar Sankhya (15 agendas), Losts distribuídos e Plano Laisa na Base — que seguem rodando em paralelo.', { x: 0.7, y: 6.95, w: 12.1, h: 0.35, fontFace: 'Arial', fontSize: 12, italic: true, color: C.roxoSoft });
        pageNum(s, 4, TOTAL_SLIDES);
    }

    // ==== SLIDE 5: COBERTURA DA META ====
    {
        const s = pres.addSlide();
        slideHeader(s, C.roxo, 'COBERTURA DA META', `${brlk(META_MRR)} · onde estamos em cada fatia`, 'Ganho realizado + Garantido no forecast + Pipeline aquecendo');
        const barY = 2.3;
        s.addText('POSIÇÃO CONSOLIDADA', { x: 0.7, y: barY, w: 12, h: 0.35, fontFace: 'Arial', fontSize: 11, bold: true, color: C.muted, charSpacing: 4 });
        const barX = 0.7, barW = 12.1, barH = 1.0;
        s.addShape('rect', { x: barX, y: barY + 0.5, w: barW, h: barH, fill: { color: '2A3050' } });
        const wGanho = barW * Math.min(ganhosMrr / META_MRR, 1);
        s.addShape('rect', { x: barX, y: barY + 0.5, w: wGanho, h: barH, fill: { color: C.verde } });
        const wGarantido = barW * Math.min(garantidoMrr / META_MRR, 1 - ganhosMrr / META_MRR);
        s.addShape('rect', { x: barX + wGanho, y: barY + 0.5, w: wGarantido, h: barH, fill: { color: C.lima, transparency: 15 } });
        const pipelineStart = barX + wGanho + wGarantido;
        const pipelineSpace = Math.max(0, barW - (wGanho + wGarantido));
        const pipelineDisplayW = Math.min(pipelineSpace, barW * (pipelineMrr / META_MRR));
        if (pipelineDisplayW > 0) {
            s.addShape('rect', { x: pipelineStart, y: barY + 0.5, w: pipelineDisplayW, h: barH, fill: { color: C.ambar, transparency: 35 } });
        }
        s.addShape('line', { x: barX + barW, y: barY + 0.4, w: 0, h: barH + 0.2, line: { color: C.text, width: 2 } });
        s.addText(`META ${brlk(META_MRR)}`, { x: barX + barW - 1.5, y: barY + 1.55, w: 1.6, h: 0.3, fontFace: 'Arial', fontSize: 10, color: C.text, bold: true, align: 'right' });

        const pctGanho = Math.round((ganhosMrr / META_MRR) * 100);
        const pctGarantido = coberturaGarantida - pctGanho;
        const pctPipelinePct = Math.round((pipelineMrr / META_MRR) * 100);
        s.addText(`${pctGanho}%`, { x: barX + 0.1, y: barY + 0.15, w: 0.8, h: 0.3, fontFace: 'Arial', fontSize: 11, bold: true, color: C.verde });
        s.addText(`${pctGarantido}%`, { x: barX + wGanho + 0.1, y: barY + 0.15, w: 0.8, h: 0.3, fontFace: 'Arial', fontSize: 11, bold: true, color: C.lima });
        s.addText(`+${pctPipelinePct}%`, { x: pipelineStart + 0.1, y: barY + 0.15, w: 0.9, h: 0.3, fontFace: 'Arial', fontSize: 11, bold: true, color: C.ambar });

        const legY = 4.2;
        const legs = [
            { color: C.verde, label: 'GANHO NO MÊS', val: brlk(ganhosMrr), desc: `${pctGanho}% da meta · ${wonThisMonth.length} deals já fechados` },
            { color: C.lima,  label: 'GARANTIDO NO FORECAST', val: brlk(garantidoMrr), desc: `${pctGarantido}% · ${garantidoDeals.length} OPOs pra fechar até fim do mês` },
            { color: C.ambar, label: 'PIPELINE (CHANCE)', val: brlk(pipelineMrr), desc: `${pipelineDeals.length} OPOs · basta ${Math.ceil((gapPraMeta / pipelineMrr) * 100)}% converter pra bater meta` }
        ];
        legs.forEach((l, i) => {
            const lx = 0.7 + i * 4.1;
            s.addShape('rect', { x: lx, y: legY, w: 3.9, h: 2.0, fill: { color: C.surface } });
            s.addShape('rect', { x: lx, y: legY, w: 0.12, h: 2.0, fill: { color: l.color } });
            s.addText(l.label, { x: lx + 0.3, y: legY + 0.2, w: 3.5, h: 0.3, fontFace: 'Arial', fontSize: 9, color: C.muted, charSpacing: 2, bold: true });
            s.addText(l.val, { x: lx + 0.3, y: legY + 0.6, w: 3.5, h: 0.7, fontFace: 'Arial', fontSize: 26, bold: true, color: l.color });
            s.addText(l.desc, { x: lx + 0.3, y: legY + 1.4, w: 3.5, h: 0.5, fontFace: 'Arial', fontSize: 11, color: C.mutedSoft });
        });
        s.addShape('rect', { x: 0.7, y: 6.6, w: 12.1, h: 0.6, fill: { color: C.roxo, transparency: 82 } });
        s.addText(`Se o time entregar o forecast garantido + ${Math.ceil((gapPraMeta / pipelineMrr) * 100)}% do pipeline, o mês fecha em ${brlk(META_MRR)}+.`, {
            x: 0.9, y: 6.6, w: 11.7, h: 0.6, fontFace: 'Arial', fontSize: 14, italic: true, bold: true, color: C.roxoSoft, valign: 'middle'
        });
        pageNum(s, 5, TOTAL_SLIDES);
    }

    // ==== SLIDE 6: META POR TIME ====
    {
        const s = pres.addSlide();
        slideHeader(s, C.ciano, 'A META POR TIME', 'Como os R$ 200k se dividem', 'Cada time tem seu funil calibrado por ticket médio e taxas históricas');
        const times = [
            { name: 'Sankhya', mrr: 106, mix: '53%', ticket: 6.0, color: C.roxo, deals: 18, opo: 45, sql: 90, sal: 225, desc: 'Deals consultivos · maior ticket médio' },
            { name: 'Novos Negócios', mrr: 64, mix: '32%', ticket: 4.5, color: C.rosa, deals: 14, opo: 40, sql: 89, sal: 254, desc: 'Funil frio · mais volume no topo' },
            { name: 'Farmer', mrr: 30, mix: '15%', ticket: 3.0, color: C.verde, deals: 10, opo: 20, sql: 33, sal: 67, desc: 'Base morna · maior taxa de conversão' }
        ];
        times.forEach((t, i) => {
            const y = 2.15 + i * 1.55;
            s.addShape('rect', { x: 0.7, y, w: 12.1, h: 1.4, fill: { color: C.surface } });
            s.addShape('rect', { x: 0.7, y, w: 0.15, h: 1.4, fill: { color: t.color } });
            s.addText(t.name, { x: 1.0, y: y + 0.15, w: 3.5, h: 0.45, fontFace: 'Arial', fontSize: 18, bold: true, color: t.color });
            s.addText('mix ' + t.mix + ' · ticket médio R$ ' + t.ticket.toFixed(1).replace('.', ',') + 'k', { x: 1.0, y: y + 0.6, w: 3.5, h: 0.3, fontFace: 'Arial', fontSize: 10, color: C.muted });
            s.addText(t.desc, { x: 1.0, y: y + 0.9, w: 3.5, h: 0.4, fontFace: 'Arial', fontSize: 10, italic: true, color: C.mutedSoft });
            s.addText('R$ ' + t.mrr + 'k', { x: 4.7, y: y + 0.15, w: 1.8, h: 0.7, fontFace: 'Arial', fontSize: 30, bold: true, color: t.color });
            s.addText('MRR · meta mês', { x: 4.7, y: y + 0.9, w: 1.8, h: 0.4, fontFace: 'Arial', fontSize: 10, color: C.muted });
            const nums = [
                { label: 'DEALS', val: t.deals },
                { label: 'PROPOSTAS', val: t.opo },
                { label: 'REUNIÕES', val: t.sql },
                { label: 'AGENDADAS', val: t.sal }
            ];
            nums.forEach((n, j) => {
                const nx = 6.7 + j * 1.55;
                s.addText(n.label, { x: nx, y: y + 0.2, w: 1.5, h: 0.3, fontFace: 'Arial', fontSize: 9, color: C.muted, charSpacing: 2, bold: true });
                s.addText(String(n.val), { x: nx, y: y + 0.5, w: 1.5, h: 0.7, fontFace: 'Arial', fontSize: 26, bold: true, color: C.text });
            });
        });
        const totalY = 2.15 + times.length * 1.55 + 0.1;
        s.addShape('rect', { x: 0.7, y: totalY, w: 12.1, h: 0.5, fill: { color: C.ciano, transparency: 82 }, line: { color: C.ciano, width: 1 } });
        s.addText('TOTAL', { x: 1.0, y: totalY, w: 3.5, h: 0.5, fontFace: 'Arial', fontSize: 13, bold: true, color: C.ciano, valign: 'middle', charSpacing: 3 });
        s.addText('R$ 200k', { x: 4.7, y: totalY, w: 1.8, h: 0.5, fontFace: 'Arial', fontSize: 15, bold: true, color: C.text, valign: 'middle' });
        [42, 105, 212, 546].forEach((v, j) => {
            const nx = 6.7 + j * 1.55;
            s.addText(String(v), { x: nx, y: totalY, w: 1.5, h: 0.5, fontFace: 'Arial', fontSize: 15, bold: true, color: C.text, valign: 'middle' });
        });
        pageNum(s, 6, TOTAL_SLIDES);
    }

    // ==== SLIDE 7: RITMO DO TIME (Reuniões + Propostas) ====
    {
        const s = pres.addSlide();
        slideHeader(s, C.ciano, 'RITMO DO TIME', `Atividade desde ${sinceLabel}`, `${totalReunioes} reuniões realizadas · ${totalPropostas} propostas enviadas por vendedor`);

        // Coluna esquerda: Reuniões
        s.addShape('rect', { x: 0.7, y: 2.15, w: 5.9, h: 4.9, fill: { color: C.surface }, line: { color: C.verde, width: 1, transparency: 70 } });
        s.addText('REUNIÕES REALIZADAS', { x: 0.9, y: 2.3, w: 5.5, h: 0.4, fontFace: 'Arial', fontSize: 12, bold: true, color: C.verde, charSpacing: 4 });
        s.addText(String(totalReunioes), { x: 0.9, y: 2.7, w: 5.5, h: 0.9, fontFace: 'Arial', fontSize: 40, bold: true, color: C.verde });
        s.addText(`total desde ${sinceLabel}`, { x: 0.9, y: 3.6, w: 5.5, h: 0.3, fontFace: 'Arial', fontSize: 10, color: C.muted });

        const maxRe = reunioesSort.length ? reunioesSort[0][1] : 1;
        const showRe = reunioesSort.slice(0, 8);
        showRe.forEach(([nome, count], i) => {
            const y = 4.05 + i * 0.35;
            const w = 3.0 * (count / maxRe);
            s.addShape('rect', { x: 0.9, y: y + 0.08, w: 3.0, h: 0.15, fill: { color: '2A3050' } });
            s.addShape('rect', { x: 0.9, y: y + 0.08, w: w, h: 0.15, fill: { color: C.verde } });
            s.addText(nome, { x: 0.9, y, w: 3.6, h: 0.28, fontFace: 'Arial', fontSize: 10, color: C.text });
            s.addText(String(count), { x: 4.6, y, w: 1.8, h: 0.28, fontFace: 'Arial', fontSize: 11, bold: true, color: C.verde, align: 'right' });
        });
        if (showRe.length === 0) {
            s.addText('(sem reuniões realizadas no período)', { x: 0.9, y: 4.2, w: 5.5, h: 0.4, fontFace: 'Arial', fontSize: 11, italic: true, color: C.muted });
        }

        // Coluna direita: Propostas
        s.addShape('rect', { x: 6.9, y: 2.15, w: 5.9, h: 4.9, fill: { color: C.surface }, line: { color: C.ambar, width: 1, transparency: 70 } });
        s.addText('PROPOSTAS ENVIADAS', { x: 7.1, y: 2.3, w: 5.5, h: 0.4, fontFace: 'Arial', fontSize: 12, bold: true, color: C.ambar, charSpacing: 4 });
        s.addText(String(totalPropostas), { x: 7.1, y: 2.7, w: 5.5, h: 0.9, fontFace: 'Arial', fontSize: 40, bold: true, color: C.ambar });
        s.addText(`total desde ${sinceLabel}`, { x: 7.1, y: 3.6, w: 5.5, h: 0.3, fontFace: 'Arial', fontSize: 10, color: C.muted });

        const maxPr = propostasSort.length ? propostasSort[0][1] : 1;
        const showPr = propostasSort.slice(0, 8);
        showPr.forEach(([nome, count], i) => {
            const y = 4.05 + i * 0.35;
            const w = 3.0 * (count / maxPr);
            s.addShape('rect', { x: 7.1, y: y + 0.08, w: 3.0, h: 0.15, fill: { color: '2A3050' } });
            s.addShape('rect', { x: 7.1, y: y + 0.08, w: w, h: 0.15, fill: { color: C.ambar } });
            s.addText(nome, { x: 7.1, y, w: 3.6, h: 0.28, fontFace: 'Arial', fontSize: 10, color: C.text });
            s.addText(String(count), { x: 10.8, y, w: 1.8, h: 0.28, fontFace: 'Arial', fontSize: 11, bold: true, color: C.ambar, align: 'right' });
        });
        if (showPr.length === 0) {
            s.addText('(sem propostas enviadas no período)', { x: 7.1, y: 4.2, w: 5.5, h: 0.4, fontFace: 'Arial', fontSize: 11, italic: true, color: C.muted });
        }

        s.addText(`Fonte: cache Ploomes · corte ${untilLabel} · regra: (a) data proposta ou (b) MRR/Setup + criação ou (c) marcador + LastUpdate`, {
            x: 0.7, y: 7.05, w: 12.1, h: 0.3, fontFace: 'Arial', fontSize: 9, italic: true, color: C.muted
        });
        pageNum(s, 7, TOTAL_SLIDES);
    }

    // ==== SLIDE 8: FORECAST GARANTIDO ====
    {
        const s = pres.addSlide();
        const opoCount = garantidoDeals.length;
        slideHeader(s, C.verde, `FORECAST GARANTIDO · ${opoCount} OPOs`, `${brl(garantidoMrr)} MRR · ${brl(garantidoSetup)} Setup`, `${coberturaGarantida}% da meta confirmada · pra fechar até fim do mês`);

        // Renderiza até 14 OPOs em 2 colunas
        const maxShow = 14;
        const opos = garantidoDeals.slice(0, maxShow);
        const rows = Math.ceil(opos.length / 2);
        const rowH = Math.min(0.54, (6.4 - 2.15) / Math.max(rows, 7));
        const startY = 2.15;
        opos.forEach((d, i) => {
            const col = Math.floor(i / rows);
            const row = i % rows;
            const x = col === 0 ? 0.7 : 6.9;
            const y = startY + row * rowH;
            s.addShape('rect', { x, y, w: 5.9, h: rowH - 0.08, fill: { color: C.surface }, line: { color: C.verde, width: 1, transparency: 80 } });
            const title = (d.Title || '').slice(0, 38);
            const mrr = getMRR(d);
            const setup = getSetup(d);
            s.addText(title, { x: x + 0.15, y: y + 0.04, w: 3.0, h: rowH - 0.15, fontFace: 'Arial', fontSize: 11, bold: true, color: C.text, valign: 'middle' });
            const setupText = setup > 0 ? brl(setup) : '—';
            s.addText(`MRR ${brl(mrr)}  ·  Setup ${setupText}`, { x: x + 3.15, y: y + 0.04, w: 2.6, h: rowH - 0.15, fontFace: 'Arial', fontSize: 9.5, color: C.muted, valign: 'middle', align: 'right' });
        });
        if (garantidoDeals.length > maxShow) {
            s.addText(`+ ${garantidoDeals.length - maxShow} outras OPOs no forecast garantido (não exibidas por espaço)`, {
                x: 0.7, y: 6.05, w: 12.1, h: 0.3, fontFace: 'Arial', fontSize: 10, italic: true, color: C.muted, align: 'center'
            });
        }

        s.addShape('rect', { x: 0.7, y: 6.4, w: 12.1, h: 0.7, fill: { color: C.verde, transparency: 85 }, line: { color: C.verde, width: 1 } });
        s.addText('SUBTOTAL GARANTIDO', { x: 0.9, y: 6.4, w: 6, h: 0.7, fontFace: 'Arial', fontSize: 13, bold: true, color: C.verde, valign: 'middle', charSpacing: 4 });
        s.addText(`MRR ${brl(garantidoMrr)}  ·  Setup ${brl(garantidoSetup)}`, { x: 6.5, y: 6.4, w: 6.1, h: 0.7, fontFace: 'Arial', fontSize: 15, bold: true, color: C.text, valign: 'middle', align: 'right' });
        pageNum(s, 8, TOTAL_SLIDES);
    }

    // ==== SLIDE 9: PIPELINE ====
    {
        const s = pres.addSlide();
        const opoCount = pipelineDeals.length;
        slideHeader(s, C.ambar, `PIPELINE · ${opoCount} OPOs`, `${brl(pipelineMrr)} MRR · ${brl(pipelineSetup)} Setup`, 'Aquecendo · sem Forecast preenchido · precisam virar Garantido em S4');
        const maxShow = 10;
        const opos = pipelineDeals.slice(0, maxShow);
        const rows = Math.ceil(opos.length / 2);
        const rowH = Math.min(0.72, (5.5 - 2.15) / Math.max(rows, 5));
        const startY = 2.15;
        opos.forEach((d, i) => {
            const col = Math.floor(i / rows);
            const row = i % rows;
            const x = col === 0 ? 0.7 : 6.9;
            const y = startY + row * rowH;
            s.addShape('rect', { x, y, w: 5.9, h: rowH - 0.1, fill: { color: C.surface }, line: { color: C.ambar, width: 1, transparency: 80 } });
            const title = (d.Title || '').slice(0, 40);
            const mrr = getMRR(d);
            const setup = getSetup(d);
            s.addText(title, { x: x + 0.15, y: y + 0.05, w: 3.4, h: rowH - 0.15, fontFace: 'Arial', fontSize: 12, bold: true, color: C.text, valign: 'middle' });
            const setupText = setup > 0 ? brl(setup) : '—';
            s.addText(`MRR ${brl(mrr)}\nSetup ${setupText}`, { x: x + 3.55, y: y + 0.02, w: 2.25, h: rowH - 0.15, fontFace: 'Arial', fontSize: 10, color: C.muted, valign: 'middle', align: 'right' });
        });
        if (pipelineDeals.length > maxShow) {
            s.addText(`+ ${pipelineDeals.length - maxShow} outras OPOs no pipeline (não exibidas por espaço)`, {
                x: 0.7, y: 5.55, w: 12.1, h: 0.3, fontFace: 'Arial', fontSize: 10, italic: true, color: C.muted, align: 'center'
            });
        }
        s.addShape('rect', { x: 0.7, y: 5.9, w: 12.1, h: 0.7, fill: { color: C.ambar, transparency: 85 }, line: { color: C.ambar, width: 1 } });
        s.addText('SUBTOTAL PIPELINE', { x: 0.9, y: 5.9, w: 6, h: 0.7, fontFace: 'Arial', fontSize: 13, bold: true, color: C.ambar, valign: 'middle', charSpacing: 4 });
        s.addText(`MRR ${brl(pipelineMrr)}  ·  Setup ${brl(pipelineSetup)}`, { x: 6.5, y: 5.9, w: 6.1, h: 0.7, fontFace: 'Arial', fontSize: 15, bold: true, color: C.text, valign: 'middle', align: 'right' });
        pageNum(s, 9, TOTAL_SLIDES);
    }

    // ==== SLIDE 10: CONTAS QUENTES ====
    {
        const s = pres.addSlide();
        slideHeader(s, C.ciano, 'CONTAS QUENTES 17/07', '8 de 15 já migraram · 53% em 7 dias', 'Movimento real das contas destacadas na semana passada');
        s.addShape('rect', { x: 0.7, y: 2.15, w: 5.9, h: 4.7, fill: { color: C.surface }, line: { color: C.verde, width: 1, transparency: 65 } });
        s.addText('✓  MIGRARAM · 8', { x: 0.9, y: 2.3, w: 5.6, h: 0.4, fontFace: 'Arial', fontSize: 12, bold: true, color: C.verde, charSpacing: 3 });
        const migradas = [
            ['Inflow (Alan)', 'Garantido S4'],
            ['EAU Distribuidora (Alan)', 'Garantido S4'],
            ['Hoya Vision Care (Lechuk)', 'Pipeline · R$ 78k'],
            ['Cofermeta (Matheus)', 'Pipeline · R$ 11k'],
            ['Ponto Duplo (Matheus)', 'Pipeline · R$ 6,3k'],
            ['Bioinstinto (Gaitoline)', 'Pipeline · R$ 7k'],
            ['Artvinco (Lechuk)', 'Pipeline · R$ 11k'],
            ['Fiação Itabaiana (Bruno)', 'Pipeline · R$ 6k']
        ];
        migradas.forEach((m, i) => {
            const y = 2.75 + i * 0.48;
            s.addText(m[0], { x: 0.9, y, w: 3.2, h: 0.4, fontFace: 'Arial', fontSize: 11, bold: true, color: C.text });
            s.addText(m[1], { x: 4.1, y, w: 2.4, h: 0.4, fontFace: 'Arial', fontSize: 10, color: C.verde, align: 'right' });
        });
        s.addShape('rect', { x: 6.9, y: 2.15, w: 5.9, h: 4.7, fill: { color: C.surface }, line: { color: C.ambar, width: 1, transparency: 65 } });
        s.addText('→  AGUARDANDO · 7', { x: 7.1, y: 2.3, w: 5.6, h: 0.4, fontFace: 'Arial', fontSize: 12, bold: true, color: C.ambar, charSpacing: 3 });
        const fora = [
            ['Costa Sul Pescados', 'Fred'],
            ['Pacheco', 'Alan'],
            ['Novelis', 'Matheus'],
            ['Boreda', 'Lechuk'],
            ['La Violeteira', 'Lechuk'],
            ['Caldo Bom', 'Lechuk'],
            ['Grupo Mancherter', 'Lechuk']
        ];
        fora.forEach((m, i) => {
            const y = 2.75 + i * 0.48;
            s.addText(m[0], { x: 7.1, y, w: 3.5, h: 0.4, fontFace: 'Arial', fontSize: 11, bold: true, color: C.text });
            s.addText(m[1], { x: 10.7, y, w: 2.0, h: 0.4, fontFace: 'Arial', fontSize: 10, color: C.muted, align: 'right' });
        });
        s.addText('Alan puxou 2 contas em 7 dias. Matheus 3. Lechuk trouxe Hoya e Artvinco (as maiores) — falta destravar mais 4 da carteira dele em S4.',
            { x: 0.7, y: 6.95, w: 12.1, h: 0.4, fontFace: 'Arial', fontSize: 12, italic: true, color: C.ciano }
        );
        pageNum(s, 10, TOTAL_SLIDES);
    }

    // ==== SLIDE 11: PRÓXIMOS PASSOS ====
    {
        const s = pres.addSlide();
        slideHeader(s, C.roxo, 'PRÓXIMOS PASSOS', 'Semana 4 · como fechamos o mês', 'Manter o ritmo que destravou a S3');
        const cards = [
            { n: '01', title: 'Fechar S3 esta semana', color: C.verde, body: 'Artesana e União Química (24/07), Doceria da Rebeka (31/07). NF diária pra não deixar escorregar. Confirmar setups pendentes de Bomixlog, Líder Aviação e Honda.' },
            { n: '02', title: 'Ativar S4 · Hoya no topo', color: C.ambar, body: 'Hoya (R$ 78k) sozinha fecha a meta. Foco Lechuk na semana. Backup: Cofermeta + Transmog + Artvinco somam R$ 33k adicionais. Qualquer 2 delas + confirmações fecha os R$ 47k restantes.' },
            { n: '03', title: 'Recuperar as 7 contas restantes', color: C.ciano, body: 'Costa Sul, Pacheco, Novelis, Boreda, La Violeteira, Caldo Bom, Mancherter. 4/7 são Lechuk — 1:1 pra desbloquear. Alan puxou 2 em 7 dias, mesmo ritmo desbloqueia todas.' },
            { n: '04', title: 'Manter o motor do topo do funil', color: C.roxo, body: 'Webinar Sankhya + Losts + Plano Laisa continuam rodando. Marketing acelera captação pra sustentar o funil de agosto. O mês fecha aqui, mas o próximo já começa em S4.' }
        ];
        cards.forEach((c, i) => {
            const col = i % 2;
            const row = Math.floor(i / 2);
            const x = 0.7 + col * 6.2;
            const y = 2.15 + row * 2.4;
            const w = 5.9, h = 2.15;
            s.addShape('rect', { x, y, w, h, fill: { color: C.surface }, line: { color: c.color, width: 1, transparency: 65 } });
            s.addShape('rect', { x, y, w: 0.15, h, fill: { color: c.color } });
            s.addText(c.n, { x: x + 0.35, y: y + 0.15, w: 0.8, h: 0.5, fontFace: 'Arial', fontSize: 20, bold: true, color: c.color });
            s.addText(c.title, { x: x + 1.2, y: y + 0.2, w: w - 1.4, h: 0.5, fontFace: 'Arial', fontSize: 15, bold: true, color: C.text });
            s.addText(c.body, { x: x + 0.35, y: y + 0.85, w: w - 0.55, h: h - 1.0, fontFace: 'Arial', fontSize: 11, color: C.mutedSoft });
        });
        s.addText('Semana 4 é decisiva — o time provou que consegue destravar o mês. Agora é execução.',
            { x: 0.7, y: 6.95, w: 12.1, h: 0.35, fontFace: 'Arial', fontSize: 13, italic: true, color: C.roxoSoft }
        );
        pageNum(s, 11, TOTAL_SLIDES);
    }

    const buffer = await pres.write({ outputType: 'nodebuffer' });
    return buffer;
}

module.exports = { generateForecastPpt };

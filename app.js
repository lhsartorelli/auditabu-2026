const $=s=>document.querySelector(s), $$=s=>[...document.querySelectorAll(s)];
const DBKEY='auditabu2026.v2', CKEY='auditabu2026.candidates'; let db=JSON.parse(localStorage.getItem(DBKEY)||'{"bus":[],"pending":{},"alerts":[]}'); let candidates=JSON.parse(localStorage.getItem(CKEY)||'{}');
const offices={'1':'Presidente','3':'Governador','5':'Senador','6':'Deputado Federal','7':'Deputado Estadual/Distrital'};
function save(){localStorage.setItem(DBKEY,JSON.stringify(db));render()}; function esc(s){return String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function pairs(txt){let out=[]; for(const p of txt.trim().split(/\s+/)){let i=p.indexOf(':'); if(i>0)out.push([p.slice(0,i),p.slice(i+1)])} return out}
function vals(ps,k){return ps.filter(x=>x[0]===k).map(x=>x[1])} function val(ps,k){return vals(ps,k)[0]||''}
function parseSeq(txt){let m=txt.match(/(?:^|\s)QRBU:(\d+):(\d+)/); return m?{total:+m[1],idx:+m[2]}:null}
function pendingKey(txt){let ps=pairs(txt); return [val(ps,'PLEI'),val(ps,'UF'),val(ps,'MUNI'),val(ps,'ZONA'),val(ps,'SECA'),val(ps,'IDUE')].join('|') || ('unknown-'+Date.now())}
function parseBU(chunks){let text=chunks.join(' '), ps=pairs(text), meta={uf:val(ps,'UF'),muni:val(ps,'MUNI'),zona:val(ps,'ZONA'),secao:val(ps,'SECA'),urna:val(ps,'IDUE'),turno:val(ps,'TURNO'),pleito:val(ps,'PLEI'),comp:val(ps,'COMP'),hashes:vals(ps,'HASH'),assi:val(ps,'ASSI')}; let votes={}, current=''; for(let [k,v] of ps){if(k==='CARG'){current=v;votes[current]??={nominal:{},branco:0,nulo:0,total:0,legenda:{}}} else if(current){if(k==='BRAN')votes[current].branco+=+v||0; else if(k==='NULO')votes[current].nulo+=+v||0; else if(k==='TOTC')votes[current].total+=+v||0; else if(/^\d+$/.test(k))votes[current].nominal[k]=(votes[current].nominal[k]||0)+(+v||0)}} return {meta,votes,rawChunks:chunks,created:new Date().toISOString()}}
function buId(b){return [b.meta.pleito,b.meta.uf,b.meta.muni,b.meta.zona,b.meta.secao,b.meta.urna,b.meta.turno].join('|')}
const SUPABASE_URL='https://mhdinwlgubmxyjldjkqb.supabase.co';
const SUPABASE_KEY='sb_publishable_P4iFEgWjDRE5Pdq1y8OLIw_TY7kQh5k';
const DEVICE_KEY='auditabu2026.device';let deviceId=localStorage.getItem(DEVICE_KEY);if(!deviceId){deviceId=(crypto.randomUUID?.()||('dev-'+Date.now()+'-'+Math.random().toString(36).slice(2)));localStorage.setItem(DEVICE_KEY,deviceId)}
async function sb(path,opt={}){let r=await fetch(SUPABASE_URL+'/rest/v1/'+path,{...opt,headers:{apikey:SUPABASE_KEY,Authorization:'Bearer '+SUPABASE_KEY,'Content-Type':'application/json',...(opt.headers||{})}});if(!r.ok)throw Error('Falha na sincronização central ('+r.status+').');let t=await r.text();return t?JSON.parse(t):null}
function centralPayload(b){let rows=[];for(const[c,v]of Object.entries(b.votes)){for(const[n,q]of Object.entries(v.nominal||{}))rows.push({cargo:c,numero:n,votos:+q||0});rows.push({cargo:c,numero:'BRAN',votos:+v.branco||0},{cargo:c,numero:'NULO',votos:+v.nulo||0},{cargo:c,numero:'TOTC',votos:+v.total||0})}return {bu_uid:b.id,pleito:b.meta.pleito,uf:b.meta.uf,municipio:b.meta.muni,zona:b.meta.zona,secao:b.meta.secao,urna:b.meta.urna,turno:b.meta.turno,integrity_status:b.integrity,signature_status:b.signature,raw_chunks:b.rawChunks,payload:b,device_id:deviceId,captured_at:b.created,votes:rows}}
async function ingestCentral(b){return sb('rpc/ingest_bu',{method:'POST',body:JSON.stringify({p_bu:centralPayload(b)})})}
async function refreshCentral(){try{for(const b of db.bus.filter(x=>x.syncPending)){try{let r=await ingestCentral(b);if(r?.status==='inserted'||r?.status==='duplicate')b.syncPending=false}catch(e){}}let rows=await sb('boletins?select=payload&order=received_at.asc');let remote=(rows||[]).map(x=>x.payload).filter(x=>x?.id);let localPending=db.bus.filter(x=>x.syncPending);let map=new Map(remote.map(x=>[x.id,x]));for(let x of localPending)if(!map.has(x.id))map.set(x.id,x);db.bus=[...map.values()];localStorage.setItem(DBKEY,JSON.stringify(db));render();$('#syncState')&&($('#syncState').textContent='Base central sincronizada • '+new Date().toLocaleTimeString('pt-BR'))}catch(e){$('#syncState')&&($('#syncState').textContent='Sem conexão com a base central — dados locais preservados.')}}
async function acceptChunk(txt){let seq=parseSeq(txt);if(!seq)throw Error('QRBU:n:x não encontrado. Confirme que o conteúdo é de um BU.');let key=pendingKey(txt),p=db.pending[key]||{total:seq.total,chunks:{}};p.total=seq.total;p.chunks[seq.idx]=txt.trim();db.pending[key]=p;if(Object.keys(p.chunks).length<p.total){save();return `QR ${seq.idx}/${p.total} recebido. Faltam ${p.total-Object.keys(p.chunks).length}.`}let chunks=[];for(let i=1;i<=p.total;i++){if(!p.chunks[i])throw Error(`Falta o QR ${i}/${p.total}`);chunks.push(p.chunks[i])}let b=parseBU(chunks);b.id=buId(b);if(db.bus.some(x=>x.id===b.id&&!x.syncPending)){delete db.pending[key];db.alerts.push({type:'duplicado',id:b.id,at:new Date().toISOString()});save();return 'BU já cadastrado: não foi somado novamente.'}b.integrity=b.meta.hashes.length?'HASH presente — cadeia registrada':'HASH não informado/identificado';b.signature=b.meta.assi?'ASSI presente — não verificada':'não identificada';try{let r=await ingestCentral(b);delete db.pending[key];if(r?.status==='duplicate'){db.alerts.push({type:'duplicado',id:b.id,at:new Date().toISOString()});await refreshCentral();return 'BU já cadastrado: não foi somado novamente.'}b.syncPending=false;db.bus=db.bus.filter(x=>x.id!==b.id);db.bus.push(b);save();return 'BU contabilizado com sucesso.'}catch(e){b.syncPending=true;db.bus=db.bus.filter(x=>x.id!==b.id);db.bus.push(b);delete db.pending[key];save();return 'BU salvo neste aparelho. Sem conexão com a base central; sincronização pendente.'}}
function candidateInfo(cargo,num,uf){let v=candidates[[cargo,num,uf].join('|')]||candidates[[cargo,num,''].join('|')]||'';if(!v)return {nome:`Opção ${num}`,partido:''};let p=v.split(' — ');return {nome:p[0]||`Opção ${num}`,partido:p.slice(1).join(' — ')||''}}
function candidateName(cargo,num,uf){let c=candidateInfo(cargo,num,uf);return c.partido?`${c.nome} — ${c.partido}`:c.nome}
function render(){ $('#mBU').textContent=db.bus.length;$('#mSec').textContent=new Set(db.bus.map(b=>[b.meta.uf,b.meta.zona,b.meta.secao].join('|'))).size;$('#mPend').textContent=Object.values(db.pending).reduce((a,p)=>a+Math.max(0,p.total-Object.keys(p.chunks).length),0);$('#mAlert').textContent=db.alerts.length;
 let cargos=[...new Set(db.bus.flatMap(b=>Object.keys(b.votes)))],oldCargo=$('#office').value;$('#office').innerHTML=cargos.length?cargos.map(c=>`<option value="${esc(c)}">${esc(offices[c]||('Cargo '+c))}</option>`).join(''):'<option value="">Sem dados</option>';if(cargos.includes(oldCargo))$('#office').value=oldCargo; let ufs=[...new Set(db.bus.map(b=>b.meta.uf).filter(Boolean))].sort();let old=$('#uf').value;$('#uf').innerHTML='<option value="">Todas</option>'+ufs.map(x=>`<option>${esc(x)}</option>`).join('');$('#uf').value=ufs.includes(old)?old:''; renderTotals();
 $('#bus').innerHTML=db.bus.slice().reverse().map(b=>`<tr><td>${esc(b.meta.uf)}</td><td>${esc(b.meta.muni)}</td><td>${esc(b.meta.zona)}</td><td>${esc(b.meta.secao)}</td><td>${esc(b.meta.urna)}</td><td>${esc(b.meta.turno)}</td><td><span class="pill good">${esc(b.integrity)}</span></td><td><span class="pill warn">${esc(b.signature)}</span></td><td>${b.syncPending?'<span class="pill warn">Pendente</span>':'<span class="pill good">Central</span>'}</td></tr>`).join('')}
function renderTotals(){let cargo=$('#office').value,uf=$('#uf').value,z=$('#zone').value.trim(), rows=db.bus.filter(b=>(!uf||b.meta.uf===uf)&&(!z||b.meta.zona===z)), t={},br=0,nu=0,total=0;for(let b of rows){let v=b.votes[cargo];if(!v)continue;for(let [n,q] of Object.entries(v.nominal))t[n]=(t[n]||0)+q;br+=v.branco;nu+=v.nulo;total+=v.total}let nominal=Object.values(t).reduce((a,b)=>a+b,0);$('#totals').innerHTML=Object.entries(t).sort((a,b)=>b[1]-a[1]).map(([n,q])=>`<tr><td>${esc(n)}</td><td>${esc(candidateName(cargo,n,uf))}</td><td><b>${q.toLocaleString('pt-BR')}</b></td><td>${nominal?(100*q/nominal).toFixed(2):'0.00'}%</td></tr>`).join('');$('#summary').textContent=`${rows.length} BU(s) filtrados • Nominais: ${nominal.toLocaleString('pt-BR')} • Brancos: ${br.toLocaleString('pt-BR')} • Nulos: ${nu.toLocaleString('pt-BR')} • TOTC informado: ${total.toLocaleString('pt-BR')}`}
$$('[data-tab]').forEach(b=>b.onclick=()=>{$$('[data-tab]').forEach(x=>x.className='btn secondary');b.className='btn primary';$$('.tab').forEach(x=>x.classList.remove('active'));$('#'+b.dataset.tab).classList.add('active')});
$('#process').onclick=async()=>{try{$('#msg').className='ok';$('#msg').textContent=await acceptChunk($('#raw').value);$('#raw').value=''}catch(e){$('#msg').className='err';$('#msg').textContent=e.message}};$('#clearRaw').onclick=()=>$('#raw').value='';$('#office').onchange=renderTotals;$('#uf').onchange=renderTotals;$('#zone').oninput=renderTotals;
let stream=null,timer=null,lastCode='',lastAt=0,scanPaused=false,requireClearFrame=false,clearFrames=0;
function stopCamera(){if(timer){clearInterval(timer);timer=null}stream?.getTracks().forEach(t=>t.stop());stream=null;$('#video').srcObject=null;$('#video').classList.add('hidden')}
async function onDecoded(raw){if(!raw||scanPaused||requireClearFrame)return;let now=Date.now();if(raw===lastCode&&now-lastAt<2500)return;lastCode=raw;lastAt=now;scanPaused=true;try{let r=await acceptChunk(raw);$('#cameraMsg').textContent=r;navigator.vibrate?.(100);if(r==='BU contabilizado com sucesso.'){alert('CAPTURA CONCLUÍDA\n\nBoletim de Urna registrado com sucesso.\n\nClique em OK e a câmera continuará aberta para o próximo BU.');lastCode=raw;lastAt=Date.now();requireClearFrame=true;clearFrames=0;$('#cameraMsg').textContent='Câmera ativa. Afaste o QR da câmera para liberar a próxima leitura.';}else if(r.startsWith('BU já cadastrado')){alert('ATENÇÃO\n\nEste Boletim de Urna já foi cadastrado e não foi somado novamente.\n\nClique em OK, afaste este QR da câmera e aponte para o próximo BU.');lastCode=raw;lastAt=Date.now();requireClearFrame=true;clearFrames=0;$('#cameraMsg').textContent='BU duplicado ignorado. Afaste o QR da câmera para liberar a próxima leitura.';}}catch(e){$('#cameraMsg').textContent=e.message}finally{setTimeout(()=>{scanPaused=false},1800)}}
$('#camera').onclick=async()=>{try{if(!window.isSecureContext)throw Error('A câmera exige HTTPS.');if(!navigator.mediaDevices?.getUserMedia)throw Error('Este navegador não disponibiliza acesso à câmera. Use “Ler QR por foto”.');stream=await navigator.mediaDevices.getUserMedia({video:{facingMode:{ideal:'environment'},width:{ideal:1280},height:{ideal:720}},audio:false});let v=$('#video');v.srcObject=stream;v.classList.remove('hidden');await v.play();$('#cameraMsg').textContent='Câmera ativa. Aponte para o QR Code do BU e mantenha-o centralizado.';let canvas=$('#scanCanvas'),ctx=canvas.getContext('2d',{willReadFrequently:true});timer=setInterval(async()=>{try{if(v.readyState<2)return;let w=v.videoWidth,h=v.videoHeight;if(!w||!h)return;let scale=Math.min(1,900/w);canvas.width=Math.round(w*scale);canvas.height=Math.round(h*scale);ctx.drawImage(v,0,0,canvas.width,canvas.height);let im=ctx.getImageData(0,0,canvas.width,canvas.height);if(typeof jsQR==='function'){let code=jsQR(im.data,im.width,im.height,{inversionAttempts:'attemptBoth'});if(requireClearFrame){if(code?.data){clearFrames=0}else if(++clearFrames>=3){requireClearFrame=false;clearFrames=0;lastCode='';$('#cameraMsg').textContent='Câmera liberada. Aponte para o próximo QR Code.'}return}if(code?.data)await onDecoded(code.data)}else{$('#cameraMsg').textContent='Leitor QR não carregado. Verifique a internet ou use “Ler QR por foto”.'}}catch(e){}},300)}catch(e){stopCamera();$('#cameraMsg').textContent=e.name==='NotAllowedError'?'Permissão da câmera negada. Autorize a câmera para este site e recarregue a página.':e.message}};
$('#stop').onclick=stopCamera;
$('#qrPhoto').onchange=async e=>{try{let f=e.target.files?.[0];if(!f)return;if(typeof jsQR!=='function')throw Error('O leitor QR não carregou.');let bmp=await createImageBitmap(f),canvas=$('#scanCanvas'),ctx=canvas.getContext('2d',{willReadFrequently:true});let scale=Math.min(1,1600/bmp.width);canvas.width=Math.round(bmp.width*scale);canvas.height=Math.round(bmp.height*scale);ctx.drawImage(bmp,0,0,canvas.width,canvas.height);let im=ctx.getImageData(0,0,canvas.width,canvas.height),code=jsQR(im.data,im.width,im.height,{inversionAttempts:'attemptBoth'});if(!code?.data)throw Error('Não consegui identificar um QR Code nessa foto.');await onDecoded(code.data)}catch(err){$('#cameraMsg').textContent=err.message}finally{e.target.value=''}};
function download(name,type,text){let a=document.createElement('a');a.href=URL.createObjectURL(new Blob([text],{type}));a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000)}
$('#json').onclick=()=>download('auditabu-2026.json','application/json',JSON.stringify(db,null,2));
$('#xlsx').onclick=()=>{if(typeof XLSX==='undefined'){alert('Biblioteca do Excel não carregou. Verifique a conexão e tente novamente.');return}let detail=[],tot={};for(let b of db.bus)for(let[c,v]of Object.entries(b.votes)){for(let[n,q]of Object.entries(v.nominal)){let ci=candidateInfo(c,n,b.meta.uf);detail.push({UF:b.meta.uf,Municipio:b.meta.muni,Zona:b.meta.zona,Secao:b.meta.secao,Urna:b.meta.urna,Turno:b.meta.turno,Cargo:offices[c]||('Cargo '+c),Numero:n,Candidato:ci.nome,Partido:ci.partido,Votos:q});let k=[b.meta.uf,c,n].join('|');if(!tot[k])tot[k]={UF:b.meta.uf,Cargo:offices[c]||('Cargo '+c),Numero:n,Candidato:ci.nome,Partido:ci.partido,Votos:0};tot[k].Votos+=q}detail.push({UF:b.meta.uf,Municipio:b.meta.muni,Zona:b.meta.zona,Secao:b.meta.secao,Urna:b.meta.urna,Turno:b.meta.turno,Cargo:offices[c]||('Cargo '+c),Numero:'',Candidato:'BRANCOS',Partido:'',Votos:v.branco});detail.push({UF:b.meta.uf,Municipio:b.meta.muni,Zona:b.meta.zona,Secao:b.meta.secao,Urna:b.meta.urna,Turno:b.meta.turno,Cargo:offices[c]||('Cargo '+c),Numero:'',Candidato:'NULOS',Partido:'',Votos:v.nulo})}let wb=XLSX.utils.book_new(),ws1=XLSX.utils.json_to_sheet(detail),ws2=XLSX.utils.json_to_sheet(Object.values(tot).sort((a,b)=>a.Cargo.localeCompare(b.Cargo)||b.Votos-a.Votos));ws1['!cols']=[{wch:5},{wch:18},{wch:8},{wch:9},{wch:12},{wch:7},{wch:25},{wch:10},{wch:35},{wch:15},{wch:12}];ws2['!cols']=[{wch:5},{wch:25},{wch:10},{wch:35},{wch:15},{wch:12}];XLSX.utils.book_append_sheet(wb,ws1,'BUs - Detalhado');XLSX.utils.book_append_sheet(wb,ws2,'Totalizacao');XLSX.writeFile(wb,'AuditaBU_2026.xlsx')};$('#wipe').onclick=()=>{if(confirm('Apagar TODOS os BUs armazenados neste aparelho?')){db={bus:[],pending:{},alerts:[]};save()}};
$('#candidateTemplate').onclick=()=>download('modelo-candidatos.csv','text/csv;charset=utf-8','\ufeffcargo,numero,nome,partido,uf\n1,00,Nome do candidato,PARTIDO,BR\n');$('#candidateFile').onchange=async e=>{let f=e.target.files?.[0];if(!f)return;let txt=await f.text(),lines=txt.replace(/^\ufeff/,'').split(/\r?\n/).filter(Boolean),head=lines.shift().split(',').map(x=>x.trim().toLowerCase());for(let l of lines){let a=l.split(','),o=Object.fromEntries(head.map((h,i)=>[h,(a[i]||'').trim()]));if(o.cargo&&o.numero)candidates[[o.cargo,o.numero,o.uf||''].join('|')]=`${o.nome||('Opção '+o.numero)}${o.partido?' — '+o.partido:''}`}localStorage.setItem(CKEY,JSON.stringify(candidates));$('#candidateMsg').textContent=`Cadastro carregado: ${Object.keys(candidates).length} registros.`;renderTotals()};$('#clearCandidates').onclick=()=>{candidates={};localStorage.removeItem(CKEY);$('#candidateMsg').textContent='Cadastro removido.';renderTotals()};

function exportControleEleitoral(){
 if(typeof XLSX==='undefined'){alert('Biblioteca do Excel não carregou.');return}
 const filtrados=db.bus.filter(b=>(!b.meta.uf||b.meta.uf==='SP')&&String(+b.meta.zona||b.meta.zona)==='54');
 const unicos=new Map();
 filtrados.forEach((b,i)=>{const k=[b.meta.pleito||'',b.meta.uf||'',b.meta.zona||'',String(+b.meta.secao||b.meta.secao),b.meta.turno||''].join('|');unicos.set(k,{b,i})});
 const bus=[...unicos.values()].sort((x,y)=>x.i-y.i).map(x=>x.b).sort((a,b)=>(+a.meta.secao||0)-(+b.meta.secao||0));
 if(!bus.length){alert('Não há BUs da Zona 054 para exportar.');return}
 const exportKey='auditabu2026.export.seq', exportSeq=(parseInt(localStorage.getItem(exportKey)||'0',10)||0)+1;
 localStorage.setItem(exportKey,String(exportSeq));
 const exportNo=String(exportSeq).padStart(3,'0'), exportStamp=new Date().toLocaleString('pt-BR');
 const bm='55855', schoolMap=window.AUDITABU_ESCOLAS||{}, voterMap=window.AUDITABU_ELEITORES||{};
 const sumNom=v=>Object.values((v||{}).nominal||{}).reduce((t,q)=>t+(+q||0),0);
 const nums=cargo=>[...new Set(bus.flatMap(b=>Object.keys((b.votes[cargo]||{}).nominal||{})))].sort((a,b)=>(+a||0)-(+b||0));
 const pres=nums('1'), gov=nums('3');
 const label=(cargo,n)=>{const x=candidateInfo(cargo,n,'SP');return ((x.nome&&!x.nome.startsWith('Opção '))?x.nome+' ':'')+'('+n+')'+(x.partido?' - '+x.partido:'')};
 const data=bus.map(b=>{const sec=String(+b.meta.secao||b.meta.secao),p=b.votes['1']||{},g=b.votes['3']||{},d=b.votes['7']||{},pv=sumNom(p),gv=sumNom(g),dv=sumNom(d);return{sec,school:schoolMap[sec]||'NÃO CADASTRADA',voters:+voterMap[sec]||0,comp:+b.meta.comp||+p.total||+g.total||+d.total||pv+(+p.branco||0)+(+p.nulo||0),pv,gv,dv,bm:+d.nominal?.[bm]||0,p,g}});

 const wb=XLSX.utils.book_new();
 const BLUE='17365D', BLUE2='244F78', WHITE='FFFFFF', GREEN='E2F0D9', YELLOW='FFF2CC', ALT='EAF1F8', BLACK='000000';
 const thin={style:'thin',color:{rgb:'B7B7B7'}}, grid={top:thin,bottom:thin,left:thin,right:thin};
 const titleStyle={font:{bold:true,color:{rgb:WHITE},sz:16},fill:{fgColor:{rgb:BLUE}},alignment:{vertical:'center'}};
 const headStyle={font:{bold:true,color:{rgb:WHITE}},fill:{fgColor:{rgb:BLUE2}},alignment:{horizontal:'center',vertical:'center',wrapText:true},border:grid};
 const center={alignment:{horizontal:'center',vertical:'center'},border:grid};
 const pct={alignment:{horizontal:'center'},numFmt:'0.0%',border:grid};
 const setStyle=(ws,range,style)=>{XLSX.utils.sheet_to_json(ws,{header:1});const r=XLSX.utils.decode_range(range);for(let R=r.s.r;R<=r.e.r;R++)for(let C=r.s.c;C<=r.e.c;C++){const a=XLSX.utils.encode_cell({r:R,c:C});if(!ws[a])ws[a]={t:'s',v:''};ws[a].s=JSON.parse(JSON.stringify(style));}};
 const col=(n)=>XLSX.utils.encode_col(n);

 // BASE DE SEÇÕES - preserva escola e eleitorado da planilha original
 const base=[['SEÇÃO','ESCOLA','ELEITORES']];
 Object.keys(voterMap).sort((a,b)=>+a-+b).forEach(sec=>base.push([+sec,schoolMap[sec]||'',+voterMap[sec]||0]));
 const wsBase=XLSX.utils.aoa_to_sheet(base);wsBase['!cols']=[{wch:10},{wch:32},{wch:12}];setStyle(wsBase,'A1:C1',headStyle);
 XLSX.utils.book_append_sheet(wb,wsBase,'Planilha2');

 // ENTRADA DE DADOS
 const entHeaders=['SEÇÃO','ESCOLA DE VOTAÇÃO','ELEITORES TOTAL','COMPARECIMENTO','PRES. VÁLIDOS'];
 pres.forEach(n=>entHeaders.push('PRES. '+label('1',n)));
 entHeaders.push('GOV. VÁLIDOS'); gov.forEach(n=>entHeaders.push('GOV. '+label('3',n)));
 entHeaders.push('DEP. EST. VÁLIDOS','BARROS MUNHOZ (55855)');
 const ent=[['CONTROLE DE VOTAÇÃO — LANÇAMENTO POR SEÇÃO — EXPORTAÇÃO Nº '+exportNo],['Dados extraídos automaticamente dos Boletins de Urna capturados. Exportação nº '+exportNo+' — '+exportStamp],['Os dados são refletidos nas abas APURAÇÃO, PARCIAIS PRESIDENTE, PARCIAIS GOVERNADOR e PARCIAIS BM.'],entHeaders];
 data.forEach(x=>{let r=[+x.sec,x.school,x.voters,x.comp,x.pv];pres.forEach(n=>r.push(+x.p.nominal?.[n]||0));r.push(x.gv);gov.forEach(n=>r.push(+x.g.nominal?.[n]||0));r.push(x.dv,x.bm);ent.push(r)});
 while(ent.length<504)ent.push(new Array(entHeaders.length).fill(''));
 const ws1=XLSX.utils.aoa_to_sheet(ent);
 ws1['!merges']=[{s:{r:0,c:0},e:{r:0,c:entHeaders.length-1}},{s:{r:1,c:0},e:{r:1,c:entHeaders.length-1}},{s:{r:2,c:0},e:{r:2,c:entHeaders.length-1}}];
 setStyle(ws1,'A1:'+col(entHeaders.length-1)+'1',titleStyle);setStyle(ws1,'A4:'+col(entHeaders.length-1)+'4',headStyle);
 for(let r=5;r<=504;r++){const a='A'+r,b='B'+r,cc='C'+r;if(!ws1[b]||ws1[b].v==='')ws1[b]={t:'n',f:'IFERROR(VLOOKUP('+a+',Planilha2!$A$2:$C$162,2,FALSE),"")'};if(!ws1[cc]||ws1[cc].v==='')ws1[cc]={t:'n',f:'IFERROR(VLOOKUP('+a+',Planilha2!$A$2:$C$162,3,FALSE),"")'};setStyle(ws1,'A'+r+':A'+r,{fill:{fgColor:{rgb:YELLOW}},border:grid});setStyle(ws1,'B'+r+':C'+r,{fill:{fgColor:{rgb:GREEN}},border:grid});setStyle(ws1,'D'+r+':'+col(entHeaders.length-1)+r,{fill:{fgColor:{rgb:YELLOW}},border:grid});}
 ws1['!cols']=entHeaders.map((h,i)=>({wch:i===1?30:(i===0?10:Math.max(13,Math.min(24,String(h).length+2)))}));
 ws1['!rows']=[{hpt:27},{hpt:20},{hpt:20},{hpt:34}];ws1['!freeze']={xSplit:0,ySplit:4};
 XLSX.utils.book_append_sheet(wb,ws1,'ENTRADA DE DADOS');

 // Índices das colunas de entrada
 const eIdx={sec:0,school:1,voters:2,comp:3,pv:4};
 let ix=5; const ePres={};pres.forEach(n=>ePres[n]=ix++);eIdx.gv=ix++;const eGov={};gov.forEach(n=>eGov[n]=ix++);eIdx.dv=ix++;eIdx.bm=ix++;

 // APURAÇÃO - 161 seções, com os mesmos três percentuais do arquivo original para cada candidato
 const apHeaders=['SEÇÃO','ESCOLA','ELEITORES','ELEITORES TOTAL','COMPAREC.','PRES. VÁLIDOS'];
 const apDef=[];
 pres.forEach(n=>{apHeaders.push('PRES. '+n,'PRES. '+n+' % COMPAR.','PRES. '+n+' % VÁL.','PRES. '+n+' % ELEITOR');apDef.push({cargo:'P',n})});
 apHeaders.push('GOV. VÁLIDOS');
 gov.forEach(n=>{apHeaders.push('GOV. '+n,'GOV. '+n+' % COMPAR.','GOV. '+n+' % VÁL.','GOV. '+n+' % ELEITOR');apDef.push({cargo:'G',n})});
 apHeaders.push('DEP. EST. VÁLIDOS','BM','BM % COMPAR.','BM % VÁL.','BM % ELEITOR');
 const ap=[apHeaders];
 Object.keys(voterMap).sort((a,b)=>+a-+b).forEach(sec=>ap.push([+sec,schoolMap[sec]||'',+voterMap[sec]||0]));
 const ws3=XLSX.utils.aoa_to_sheet(ap);setStyle(ws3,'A1:'+col(apHeaders.length-1)+'1',headStyle);
 const inputRange="$A$5:$A$504";
 const sumInput=(row,inputCol)=>'IF(COUNTIF(\'ENTRADA DE DADOS\'!'+inputRange+',A'+row+')=0,"",SUMIF(\'ENTRADA DE DADOS\'!'+inputRange+',A'+row+',\'ENTRADA DE DADOS\'!$'+col(inputCol)+'$5:$'+col(inputCol)+'$504))';
 for(let r=2;r<=162;r++){
   ws3['D'+r]={t:'n',f:'IF(COUNTIF(\'ENTRADA DE DADOS\'!'+inputRange+',A'+r+')=0,"",C'+r+')'};
   ws3['E'+r]={t:'n',f:sumInput(r,eIdx.comp)};ws3['F'+r]={t:'n',f:sumInput(r,eIdx.pv)};
   let ac=6;
   pres.forEach(n=>{const v=col(ac)+r,pc=col(ac+1)+r,pv=col(ac+2)+r,pe=col(ac+3)+r;ws3[v]={t:'n',f:sumInput(r,ePres[n])};ws3[pc]={t:'n',f:'IFERROR('+v+'/$E'+r+',0)',z:'0.0%'};ws3[pv]={t:'n',f:'IFERROR('+v+'/$F'+r+',0)',z:'0.0%'};ws3[pe]={t:'n',f:'IFERROR('+v+'/$C'+r+',0)',z:'0.0%'};ac+=4});
   ws3[col(ac)+r]={t:'n',f:sumInput(r,eIdx.gv)};const govValidCol=col(ac);ac++;
   gov.forEach(n=>{const v=col(ac)+r,pc=col(ac+1)+r,pv=col(ac+2)+r,pe=col(ac+3)+r;ws3[v]={t:'n',f:sumInput(r,eGov[n])};ws3[pc]={t:'n',f:'IFERROR('+v+'/$E'+r+',0)',z:'0.0%'};ws3[pv]={t:'n',f:'IFERROR('+v+'/$'+govValidCol+r+',0)',z:'0.0%'};ws3[pe]={t:'n',f:'IFERROR('+v+'/$C'+r+',0)',z:'0.0%'};ac+=4});
   ws3[col(ac)+r]={t:'n',f:sumInput(r,eIdx.dv)};const depValidCol=col(ac);ac++;
   const bv=col(ac)+r,bpc=col(ac+1)+r,bpv=col(ac+2)+r,bpe=col(ac+3)+r;ws3[bv]={t:'n',f:sumInput(r,eIdx.bm)};ws3[bpc]={t:'n',f:'IFERROR('+bv+'/$E'+r+',0)',z:'0.0%'};ws3[bpv]={t:'n',f:'IFERROR('+bv+'/$'+depValidCol+r+',0)',z:'0.0%'};ws3[bpe]={t:'n',f:'IFERROR('+bv+'/$C'+r+',0)',z:'0.0%'};
 }
 setStyle(ws3,'A2:'+col(apHeaders.length-1)+'162',{border:grid,alignment:{vertical:'center'}});
 for(let C=0;C<apHeaders.length;C++)if(apHeaders[C].includes('%'))for(let r=2;r<=162;r++){const a=col(C)+r;if(ws3[a])ws3[a].s={...center,numFmt:'0.0%'};}
 ws3['!cols']=apHeaders.map((h,i)=>({wch:i===1?28:(String(h).includes('%')?13:Math.max(10,Math.min(18,String(h).length+1)))}));ws3['!freeze']={xSplit:2,ySplit:1};
 XLSX.utils.book_append_sheet(wb,ws3,'APURAÇÃO');

 // PARCIAIS POR ESCOLA — separados em 3 abas: PRESIDENTE, GOVERNADOR e BM
 const schools=[...new Set(Object.values(schoolMap))].sort();
 const apEnd=162, schoolCol="'APURAÇÃO'!$B$2:$B$"+apEnd, compCol="'APURAÇÃO'!$E$2:$E$"+apEnd;
 const apColByHeader={};apHeaders.forEach((h,i)=>apColByHeader[h]=col(i));

 function criarParcialPorEscola(tipo){
   let ph=['ESCOLA','TOTAL DE SEÇÕES','SEÇÕES APURADAS','ELEITORES DA ESCOLA','COMPARECIMENTO'];
   let titulo='', sheetName='';
   if(tipo==='P'){
     titulo='RESULTADOS PARCIAIS POR ESCOLA — PRESIDENTE — EXPORTAÇÃO Nº '+exportNo;
     sheetName='PARCIAIS PRESIDENTE';
     ph.push('PRES. VÁLIDOS');
     pres.forEach(n=>ph.push('PRES. '+n,'PRES. '+n+' % COMPAR.','PRES. '+n+' % VÁL.','PRES. '+n+' % ELEITOR'));
   }else if(tipo==='G'){
     titulo='RESULTADOS PARCIAIS POR ESCOLA — GOVERNADOR — EXPORTAÇÃO Nº '+exportNo;
     sheetName='PARCIAIS GOVERNADOR';
     ph.push('GOV. VÁLIDOS');
     gov.forEach(n=>ph.push('GOV. '+n,'GOV. '+n+' % COMPAR.','GOV. '+n+' % VÁL.','GOV. '+n+' % ELEITOR'));
   }else{
     titulo='RESULTADOS PARCIAIS POR ESCOLA — BARROS MUNHOZ — EXPORTAÇÃO Nº '+exportNo;
     sheetName='PARCIAIS BM';
     ph.push('DEP. EST. VÁLIDOS','BM','BM % COMPAR.','BM % VÁL.','BM % ELEITOR');
   }
   ph.push('% SEÇÕES APURADAS');

   const par=[[titulo],['Atualização automática conforme os Boletins de Urna capturados | '+exportStamp],['Percentuais calculados sobre Comparecimento, Votos Válidos do cargo e Eleitores da escola.'],[],ph];
   schools.forEach(s=>par.push([s]));
   par.push(['TOTAL GERAL']);

   const ws=XLSX.utils.aoa_to_sheet(par);
   ws['!merges']=[{s:{r:0,c:0},e:{r:0,c:ph.length-1}},{s:{r:1,c:0},e:{r:1,c:ph.length-1}},{s:{r:2,c:0},e:{r:2,c:ph.length-1}}];
   setStyle(ws,'A1:'+col(ph.length-1)+'1',titleStyle);
   setStyle(ws,'A5:'+col(ph.length-1)+'5',headStyle);

   const sumAp=(r,pc,head)=>{
     const src=apColByHeader[head];
     ws[col(pc)+r]={t:'n',f:'SUMIF('+schoolCol+',A'+r+',\'APURAÇÃO\'!$'+src+'$2:$'+src+'$'+apEnd+')'};
     return col(pc)+r;
   };

   for(let ri=0;ri<schools.length;ri++){
     const r=6+ri;
     ws['B'+r]={t:'n',f:'COUNTIF(Planilha2!$B$2:$B$162,A'+r+')'};
     ws['C'+r]={t:'n',f:'COUNTIFS('+schoolCol+',A'+r+','+compCol+',"<>")'};
     ws['D'+r]={t:'n',f:'SUMIF(Planilha2!$B$2:$B$162,A'+r+',Planilha2!$C$2:$C$162)'};
     ws['E'+r]={t:'n',f:'SUMIF('+schoolCol+',A'+r+','+compCol+')'};
     let pc=5;

     if(tipo==='P'){
       const valid=sumAp(r,pc++,'PRES. VÁLIDOS');
       pres.forEach(n=>{
         const v=sumAp(r,pc++,'PRES. '+n);
         ws[col(pc++)+r]={t:'n',f:'IFERROR('+v+'/$E'+r+',0)',z:'0.0%'};
         ws[col(pc++)+r]={t:'n',f:'IFERROR('+v+'/'+valid+',0)',z:'0.0%'};
         ws[col(pc++)+r]={t:'n',f:'IFERROR('+v+'/$D'+r+',0)',z:'0.0%'};
       });
     }else if(tipo==='G'){
       const valid=sumAp(r,pc++,'GOV. VÁLIDOS');
       gov.forEach(n=>{
         const v=sumAp(r,pc++,'GOV. '+n);
         ws[col(pc++)+r]={t:'n',f:'IFERROR('+v+'/$E'+r+',0)',z:'0.0%'};
         ws[col(pc++)+r]={t:'n',f:'IFERROR('+v+'/'+valid+',0)',z:'0.0%'};
         ws[col(pc++)+r]={t:'n',f:'IFERROR('+v+'/$D'+r+',0)',z:'0.0%'};
       });
     }else{
       const valid=sumAp(r,pc++,'DEP. EST. VÁLIDOS');
       const v=sumAp(r,pc++,'BM');
       ws[col(pc++)+r]={t:'n',f:'IFERROR('+v+'/$E'+r+',0)',z:'0.0%'};
       ws[col(pc++)+r]={t:'n',f:'IFERROR('+v+'/'+valid+',0)',z:'0.0%'};
       ws[col(pc++)+r]={t:'n',f:'IFERROR('+v+'/$D'+r+',0)',z:'0.0%'};
     }
     ws[col(pc)+r]={t:'n',f:'IFERROR($C'+r+'/$B'+r+',0)',z:'0.0%'};
     const fill=ri%2?WHITE:ALT;
     setStyle(ws,'A'+r+':'+col(ph.length-1)+r,{fill:{fgColor:{rgb:fill}},border:grid});
   }

   const tr=6+schools.length;
   for(let C=1;C<ph.length;C++){
     const L=col(C), h=ph[C];
     if(C<=4){
       ws[L+tr]={t:'n',f:'SUM('+L+'6:'+L+(tr-1)+')'};
     }else if(h==='% SEÇÕES APURADAS'){
       ws[L+tr]={t:'n',f:'IFERROR(C'+tr+'/B'+tr+',0)',z:'0.0%'};
     }else if(h.includes('%')){
       const voteHead=h.split(' % ')[0], voteC=ph.indexOf(voteHead), vL=col(voteC);
       if(h.includes('% COMPAR.')) ws[L+tr]={t:'n',f:'IFERROR('+vL+tr+'/E'+tr+',0)',z:'0.0%'};
       else if(h.includes('% ELEITOR')) ws[L+tr]={t:'n',f:'IFERROR('+vL+tr+'/D'+tr+',0)',z:'0.0%'};
       else {
         let den='';
         if(tipo==='P') den=col(ph.indexOf('PRES. VÁLIDOS'));
         else if(tipo==='G') den=col(ph.indexOf('GOV. VÁLIDOS'));
         else den=col(ph.indexOf('DEP. EST. VÁLIDOS'));
         ws[L+tr]={t:'n',f:'IFERROR('+vL+tr+'/'+den+tr+',0)',z:'0.0%'};
       }
     }else{
       ws[L+tr]={t:'n',f:'SUM('+L+'6:'+L+(tr-1)+')'};
     }
   }

   setStyle(ws,'A'+tr+':'+col(ph.length-1)+tr,{font:{bold:true,color:{rgb:WHITE}},fill:{fgColor:{rgb:BLUE}},border:grid,alignment:{horizontal:'center'}});
   for(let C=0;C<ph.length;C++) if(ph[C].includes('%')) for(let r=6;r<=tr;r++){
     const a=col(C)+r;if(ws[a])ws[a].s={...(ws[a].s||{}),numFmt:'0.0%',alignment:{horizontal:'center'},border:grid};
   }
   ws['!cols']=ph.map((h,i)=>({wch:i===0?28:(String(h).includes('%')?13:Math.max(12,Math.min(18,String(h).length+1)))}));
   ws['!rows']=[{hpt:27},{hpt:20},{hpt:22},{hpt:8},{hpt:38}];
   ws['!freeze']={xSplit:1,ySplit:5};
   XLSX.utils.book_append_sheet(wb,ws,sheetName);
 }

 criarParcialPorEscola('P');
 criarParcialPorEscola('G');
 criarParcialPorEscola('BM');

 // Ordem das abas
 wb.SheetNames=['ENTRADA DE DADOS','PARCIAIS PRESIDENTE','PARCIAIS GOVERNADOR','PARCIAIS BM','APURAÇÃO','Planilha2'];
 const ordered={};wb.SheetNames.forEach(n=>ordered[n]=wb.Sheets[n]);wb.Sheets=ordered;
 XLSX.writeFile(wb,'CONTROLE_VOTACAO_PRES_GOV_BM_EXP_'+exportNo+'.xlsx',{cellStyles:true});
}
if('serviceWorker'in navigator)navigator.serviceWorker.register('sw.js').catch(()=>{});render();refreshCentral();setInterval(refreshCentral,5000);window.addEventListener('focus',refreshCentral);

const authClient=(window.supabase&&window.supabase.createClient)?window.supabase.createClient(SUPABASE_URL,SUPABASE_KEY):null;
async function loadAdmin(){
 if(!authClient)return;
 const {data:{session}}=await authClient.auth.getSession();
 if(!session){$('#adminLogin')?.classList.remove('hidden');$('#adminPanel')?.classList.add('hidden');return}
 const {data:ok}=await authClient.from('admin_users').select('email').eq('email',session.user.email).maybeSingle();
 if(!ok){$('#adminMsg').textContent='Conta autenticada, mas este e-mail ainda não está autorizado como administrador.';await authClient.auth.signOut();return}
 $('#adminLogin').classList.add('hidden');$('#adminPanel').classList.remove('hidden');$('#adminWho').textContent=session.user.email;
 const {data, error}=await authClient.from('boletins').select('received_at,uf,municipio,zona,secao,urna,device_id').order('received_at',{ascending:false});
 if(error){$('#adminMsg').textContent='Não foi possível carregar a base central.';return}
 $('#adminBU').textContent=data.length;$('#adminSec').textContent=new Set(data.map(x=>[x.uf,x.zona,x.secao].join('|'))).size;
 $('#adminRows').innerHTML=data.map(x=>'<tr><td>'+esc(new Date(x.received_at).toLocaleString('pt-BR'))+'</td><td>'+esc(x.uf)+'</td><td>'+esc(x.municipio)+'</td><td>'+esc(x.zona)+'</td><td>'+esc(x.secao)+'</td><td>'+esc(x.urna)+'</td><td>'+esc(x.device_id)+'</td></tr>').join('');
 $('#adminMsg').textContent='Base central carregada.';
}
if($('#adminEnter'))$('#adminEnter').onclick=async()=>{try{let email=$('#adminEmail').value.trim(),password=$('#adminPass').value;let {error}=await authClient.auth.signInWithPassword({email,password});if(error)throw error;await loadAdmin()}catch(e){$('#adminMsg').textContent='Não foi possível entrar: '+e.message}};
if($('#adminCreate'))$('#adminCreate').onclick=async()=>{const email=$('#adminEmail').value.trim(),password=$('#adminPass').value;if(!email||!password){$('#adminMsg').textContent='Informe o e-mail e a senha que deseja criar.';return}if(password.length<6){$('#adminMsg').textContent='A senha deve ter pelo menos 6 caracteres.';return}const {data,error}=await authClient.auth.signUp({email,password,options:{emailRedirectTo:'https://lhsartorelli.github.io/auditabu-2026/'}});if(error){$('#adminMsg').textContent='Não foi possível criar a senha: '+error.message;return}if(data.session){$('#adminMsg').textContent='Senha criada. Acessando o painel…';await loadAdmin()}else $('#adminMsg').textContent='Cadastro realizado. Verifique o e-mail para confirmar a conta e depois clique em Entrar.'};
if($('#adminForgot'))$('#adminForgot').onclick=async()=>{const email=$('#adminEmail').value.trim();if(!email){$('#adminMsg').textContent='Informe primeiro seu e-mail.';return}const {error}=await authClient.auth.resetPasswordForEmail(email,{redirectTo:'https://lhsartorelli.github.io/auditabu-2026/'});$('#adminMsg').textContent=error?('Não foi possível enviar a recuperação: '+error.message):'Link de recuperação enviado para '+email+'.'};
if($('#adminExit'))$('#adminExit').onclick=async()=>{await authClient.auth.signOut();await loadAdmin()};
if($('#adminRefresh'))$('#adminRefresh').onclick=loadAdmin;
setTimeout(loadAdmin,0);

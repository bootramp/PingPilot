const $=s=>document.querySelector(s),api=(url,options={})=>fetch(url,{headers:{"Content-Type":"application/json",...(options.headers||{})},...options}).then(async response=>{let data=await response.json().catch(()=>null);if(!response.ok)throw Error(data?.error||"Request failed");return data});
let globalState={targets:[]},editingId=null;
const esc=value=>String(value??"").replace(/[&<>"']/g,char=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[char]));
function toast(message){let box=$("#globalToast");box.textContent=message;box.style.display="block";setTimeout(()=>box.style.display="none",3200)}
function flag(code){let clean=String(code||"").toUpperCase().replace(/[^A-Z]/g,"").slice(0,2);return clean.length===2?[...clean].map(letter=>String.fromCodePoint(127397+letter.charCodeAt(0))).join(""):"🌐"}
function meter(value){return value==null?"—":Number(value).toFixed(1)+"%"}
function render(data){
 data.targets.sort((a,b)=>{
  const rank=status=>({ONLINE:3,DEGRADED:2,UNKNOWN:1,OFFLINE:0}[status]??0);
  let byStatus=rank(b.status)-rank(a.status);if(byStatus)return byStatus;
  let byAvailability=(b.availability??-1)-(a.availability??-1);if(byAvailability)return byAvailability;
  let byLatency=(a.last_ms??Number.MAX_SAFE_INTEGER)-(b.last_ms??Number.MAX_SAFE_INTEGER);if(byLatency)return byLatency;
  let byFailures=(a.fail_count??0)-(b.fail_count??0);if(byFailures)return byFailures;
  return String(a.country).localeCompare(String(b.country))||String(a.name).localeCompare(String(b.name));
 });
 globalState=data;let stats=data.stats||{};
 $("#globalTotal").textContent=stats.total??0;$("#globalOnline").textContent=stats.online??0;$("#globalOffline").textContent=stats.offline??0;$("#globalAvailability").textContent=meter(stats.availability);
 $("#globalGrid").innerHTML=data.targets.map(target=>`<article class="country-card ${esc(target.status)}" data-id="${target.id}"><div class="country-card-top"><span class="country-flag">${flag(target.country_code)}</span><div><small>${esc(target.country||"Unassigned")}</small><h3>${esc(target.name)}</h3></div><span class="global-status">${esc(target.status)}</span></div><p class="country-host" title="${esc(target.host)}">${esc(target.host)}</p><div class="country-metrics"><span><small>Availability</small><b>${meter(target.availability)}</b></span><span><small>Latency</small><b>${target.last_ms==null?"—":Math.round(target.last_ms)+" ms"}</b></span><span><small>Checks</small><b>${target.ok_count} / ${target.fail_count}</b></span></div><div class="country-detail"><i></i><span title="${esc(target.detail)}">${esc(target.detail||"Waiting for first check…")}</span></div><footer><span>${esc(target.protocol)}${target.port?" · "+target.port:""}</span><label class="global-toggle"><input type="checkbox" data-enabled="${target.id}" ${target.enabled?"checked":""}> Enabled</label><button data-edit="${target.id}">Edit</button><button class="delete" data-delete="${target.id}">Delete</button></footer></article>`).join("")||'<p class="hint">No global endpoints. Add one to start monitoring.</p>';
 document.querySelectorAll("[data-edit]").forEach(button=>button.onclick=()=>openEdit(+button.dataset.edit));
 document.querySelectorAll("[data-delete]").forEach(button=>button.onclick=async()=>{if(!confirm("Delete this global endpoint?"))return;try{await api("/api/global/targets/"+button.dataset.delete,{method:"DELETE"});await refresh()}catch(error){toast(error.message)}});
 document.querySelectorAll("[data-enabled]").forEach(input=>input.onchange=async()=>{let target=globalState.targets.find(item=>item.id===+input.dataset.enabled);if(!target)return;try{await api("/api/global/targets/"+target.id,{method:"PUT",body:JSON.stringify({...target,enabled:input.checked})});await refresh()}catch(error){input.checked=!input.checked;toast(error.message)}});
}
async function refresh(){try{render(await api("/api/global/state"))}catch(error){toast(error.message)}}
function setPort(){let protocol=$("#globalProtocol").value;if(protocol==="HTTPS")$("#globalPort").value=443;if(protocol==="HTTP")$("#globalPort").value=80;if(protocol==="PING")$("#globalPort").value=0}
function openEdit(id){
 let target=globalState.targets.find(item=>item.id===id);if(!target)return;editingId=id;
 $("#globalTitle").textContent="Edit global endpoint";$("#globalCountry").value=target.country;$("#globalCode").value=target.country_code;$("#globalName").value=target.name;$("#globalHost").value=target.host;$("#globalProtocol").value=target.protocol;$("#globalPort").value=target.port||0;$("#globalEnabled").checked=target.enabled;$("#globalDialog").showModal();
}
$("#globalAdd").onclick=()=>{editingId=null;$("#globalForm").reset();$("#globalTitle").textContent="Add global endpoint";$("#globalProtocol").value="HTTPS";$("#globalPort").value=443;$("#globalEnabled").checked=true;$("#globalDialog").showModal()};
$("#globalProtocol").onchange=setPort;
$("#globalForm").onsubmit=async event=>{event.preventDefault();let payload={country:$("#globalCountry").value,country_code:$("#globalCode").value,name:$("#globalName").value,host:$("#globalHost").value,protocol:$("#globalProtocol").value,port:$("#globalPort").value,enabled:$("#globalEnabled").checked};try{await api(editingId?"/api/global/targets/"+editingId:"/api/global/targets",{method:editingId?"PUT":"POST",body:JSON.stringify(payload)});$("#globalDialog").close();await refresh();toast(editingId?"Global endpoint updated":"Global endpoint added")}catch(error){toast(error.message)}};
document.querySelectorAll(".close,.cancel").forEach(button=>button.onclick=()=>button.closest("dialog").close());
refresh();setInterval(refresh,1000);

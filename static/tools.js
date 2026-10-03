const $=s=>document.querySelector(s),api=(url,options={})=>fetch(url,{cache:"no-store",headers:{"Content-Type":"application/json",...(options.headers||{})},...options}).then(async response=>{let data=await response.json().catch(()=>null);if(!response.ok)throw Error(data?.error||"Request failed");return data});
function toast(message){let box=$("#toolToast");box.textContent=message;box.style.display="block";setTimeout(()=>box.style.display="none",3800)}
async function status(){try{let state=await api("/api/tools/status");$("#toolStatus").innerHTML=`<span class="${state.nslookup?"ready":"missing"}">nslookup ${state.nslookup?"ready":"missing"}</span><span class="${state.nmap?"ready":"missing"}">nmap ${state.nmap?"ready":"missing"}</span><small>${state.note}</small>`}catch(error){$("#toolStatus").textContent=error.message}}
async function run(formId,outputId,url,payload){
 let form=$(formId),button=form.querySelector("button[type=submit]"),output=$(outputId),label=button.textContent;button.disabled=true;button.textContent="Running…";output.textContent="Running diagnostic on PingPilot server…";
 try{let result=await api(url+"?run="+Date.now(),{method:"POST",body:JSON.stringify(payload())});output.textContent=result.output||"No output returned";toast("Completed for "+result.target+(result.resolved_address?" · "+result.resolved_address:""))}catch(error){output.textContent="Error: "+error.message;toast(error.message)}finally{button.disabled=false;button.textContent=label}
}
$("#nslookupForm").onsubmit=event=>{event.preventDefault();run("#nslookupForm","#nsOutput","/api/tools/nslookup",()=>({host:$("#nsHost").value}))};
$("#nmapForm").onsubmit=event=>{event.preventDefault();run("#nmapForm","#nmapOutput","/api/tools/nmap",()=>({host:$("#nmapHost").value,scope:$("#nmapScope").value,ports:$("#nmapPorts").value}))};
$("#nmapScope").onchange=()=>{let custom=$("#nmapScope").value==="custom";$("#nmapPorts").required=custom;$("#nmapPorts").placeholder=custom?"e.g. 22,80,443 or 1-1024":"Optional: entering ports automatically uses Custom"};
status();

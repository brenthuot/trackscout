import { useState, useEffect, useRef, useMemo } from "react";
import * as d3 from "d3";

// ── THEME — Syracuse University Official Design Tokens ────────────────────────
// Source: https://designsystem.syr.edu/documentation/design-tokens/color/
const T = {
  // SU Primary
  orange:    "#F76900",  // $su-orange-primary
  orangeD:   "#D74100",  // $su-orange-dark
  orangeL:   "#FF8E00",  // $su-orange-light
  orangeM:   "#FF431B",  // $su-orange-medium
  orangeGlow:"rgba(247,105,0,0.10)",
  white:     "#FFFFFF",  // $white
  black:     "#000000",  // $black

  // White-based UI backgrounds — SU gray tints
  bg:        "#FFFFFF",  // pure white
  bgPanel:   "#F7F7F8",  // $su-gray-light-10
  bgCard:    "#EFF0F1",  // $su-gray-light-20
  border:    "#D4D6D9",  // $su-gray-medium-30
  borderH:   "#9BA0A6",  // $su-gray-medium-70
  muted:     "#707780",  // $su-gray-medium
  dim:       "#ADB3B8",  // $su-gray-light
  offWhite:  "#404040",  // $su-gray-dark (readable text on white)

  // Semantic distance colors — all from SU palette
  green:  "#2B72D7",  // $su-blue-light    (local, cool = close)
  yellow: "#FF8E00",  // $su-orange-light  (regional)
  red:    "#FF431B",  // $su-orange-medium (extreme, hot = far)

  // Supporting
  blueP:  "#000E54",  // $su-blue-primary
  blueL:  "#2B72D7",  // $su-blue-light
  blueM:  "#203299",  // $su-blue-medium
  grayM:  "#707780",  // $su-gray-medium
  grayL:  "#ADB3B8",  // $su-gray-light
};

// ── EVENT CONFIG ──────────────────────────────────────────────────────────────
const EVENTS_CFG = [
  {id:"60m",    label:"60m",           season:"indoor" },
  {id:"60mH",   label:"60mH",          season:"indoor" },
  {id:"100m",   label:"100m",          season:"outdoor"},
  {id:"200m",   label:"200m",          season:"both"   },
  {id:"400m",   label:"400m",          season:"both"   },
  {id:"800m",   label:"800m",          season:"both"   },
  {id:"1500m",  label:"1500m",         season:"both"   },
  {id:"Mile",   label:"Mile",          season:"both"   },
  {id:"3000m",  label:"3000m",         season:"indoor" },
  {id:"5000m",  label:"5000m",         season:"both"   },
  {id:"10000m", label:"10000m",        season:"outdoor"},
  {id:"110mH",  label:"110mH",         season:"outdoor"},
  {id:"400mH",  label:"400mH",         season:"outdoor"},
  {id:"LJ",     label:"Long Jump",     season:"both"   },
  {id:"TJ",     label:"Triple Jump",   season:"both"   },
  {id:"HJ",     label:"High Jump",     season:"both"   },
  {id:"PV",     label:"Pole Vault",    season:"both"   },
  {id:"SP",     label:"Shot Put",      season:"both"   },
  {id:"WT",     label:"Weight Throw",  season:"indoor" },
  {id:"DT",     label:"Discus",        season:"outdoor"},
  {id:"JT",     label:"Javelin",       season:"outdoor"},
  {id:"Pent",   label:"Pentathlon",    season:"indoor" },
  {id:"Hept",   label:"Heptathlon",    season:"outdoor"},
  {id:"4x100",  label:"4×100m",        season:"outdoor"},
  {id:"4x400",  label:"4×400m",        season:"both"   },
  {id:"XC",     label:"Cross Country", season:"outdoor"},
];

// ── CONFERENCE / COLLEGE DATA ─────────────────────────────────────────────────
const CONF_DATA = {
  "SEC":     { colleges:["Alabama","Arkansas","Auburn","Florida","Georgia","Kentucky","LSU","Mississippi State","Missouri","Ole Miss","South Carolina","Tennessee","Texas A&M","Vanderbilt"],
               coords:{Alabama:[33.211,-87.535],Arkansas:[36.068,-94.174],Auburn:[32.603,-85.481],Florida:[29.651,-82.325],Georgia:[33.958,-83.376],Kentucky:[38.029,-84.504],LSU:[30.413,-91.180],"Mississippi State":[33.456,-88.789],Missouri:[38.952,-92.328],"Ole Miss":[34.365,-89.538],"South Carolina":[33.996,-81.027],Tennessee:[35.955,-83.923],"Texas A&M":[30.618,-96.340],Vanderbilt:[36.144,-86.803]} },
  "Big Ten": { colleges:["Illinois","Indiana","Iowa","Maryland","Michigan","Michigan State","Minnesota","Nebraska","Northwestern","Ohio State","Penn State","Purdue","Rutgers","Wisconsin"],
               coords:{Illinois:[40.102,-88.228],Indiana:[39.165,-86.526],Iowa:[41.661,-91.534],Maryland:[38.986,-76.943],Michigan:[42.278,-83.738],"Michigan State":[42.731,-84.481],Minnesota:[44.974,-93.228],Nebraska:[40.820,-96.706],Northwestern:[42.055,-87.675],"Ohio State":[40.006,-83.021],"Penn State":[40.797,-77.860],Purdue:[40.425,-86.913],Rutgers:[40.500,-74.450],Wisconsin:[43.076,-89.412]} },
  "ACC":     { colleges:["Boston College","Clemson","Duke","Florida State","Georgia Tech","Louisville","Miami","NC State","North Carolina","Notre Dame","Pittsburgh","Syracuse","Virginia","Virginia Tech","Wake Forest"],
               coords:{"Boston College":[42.337,-71.168],Clemson:[34.677,-82.837],Duke:[36.001,-78.939],"Florida State":[30.441,-84.298],"Georgia Tech":[33.776,-84.396],Louisville:[38.211,-85.758],Miami:[25.756,-80.371],"NC State":[35.786,-78.686],"North Carolina":[35.905,-79.047],"Notre Dame":[41.701,-86.238],Pittsburgh:[40.444,-79.960],Syracuse:[43.036,-76.134],Virginia:[38.033,-78.508],"Virginia Tech":[37.228,-80.421],"Wake Forest":[36.134,-80.274]} },
  "Big 12":  { colleges:["Baylor","BYU","Iowa State","Kansas","Kansas State","Oklahoma State","TCU","Texas","Texas Tech","West Virginia"],
               coords:{Baylor:[31.548,-97.116],BYU:[40.252,-111.649],"Iowa State":[42.026,-93.648],Kansas:[38.971,-95.253],"Kansas State":[39.191,-96.578],"Oklahoma State":[36.127,-97.068],TCU:[32.710,-97.363],Texas:[30.284,-97.735],"Texas Tech":[33.584,-101.875],"West Virginia":[39.635,-79.954]} },
  "Pac-12":  { colleges:["Arizona","Arizona State","California","Colorado","Oregon","Oregon State","Stanford","UCLA","USC","Utah","Washington","Washington State"],
               coords:{Arizona:[32.232,-110.953],"Arizona State":[33.424,-111.928],California:[37.872,-122.260],Colorado:[40.007,-105.266],Oregon:[44.045,-123.073],"Oregon State":[44.564,-123.278],Stanford:[37.427,-122.170],UCLA:[34.068,-118.445],USC:[34.022,-118.285],Utah:[40.762,-111.836],Washington:[47.655,-122.303],"Washington State":[46.730,-117.158]} },
  "Big East": { colleges:["Butler","Connecticut","Georgetown","Marquette","Providence","Seton Hall","Villanova"],
                coords:{Butler:[39.839,-86.172],Connecticut:[41.808,-72.253],Georgetown:[38.907,-77.072],Marquette:[43.038,-87.930],Providence:[41.826,-71.403],"Seton Hall":[40.746,-74.236],Villanova:[40.036,-75.343]} },
};
const ALL_CONFERENCES = Object.keys(CONF_DATA);
const getConfColleges = (conf) => conf ? (CONF_DATA[conf]?.colleges ?? []) : ALL_CONFERENCES.flatMap(c => CONF_DATA[c].colleges).sort();
const getCollegeCoords = (college, conf) => {
  const search = conf ? [CONF_DATA[conf]] : Object.values(CONF_DATA);
  for (const d of search) if (d.coords[college]) return d.coords[college];
  return [39.5, -98.35];
};
const getCollegeConf = (college) => { for (const [c,d] of Object.entries(CONF_DATA)) if (d.colleges.includes(college)) return c; return ""; };

// ── CITIES ─────────────────────────────────────────────────────────────────────
const CITIES = [
  {n:"Atlanta, GA",c:[33.749,-84.388]},{n:"Houston, TX",c:[29.760,-95.370]},{n:"Dallas, TX",c:[32.776,-96.797]},
  {n:"Chicago, IL",c:[41.878,-87.630]},{n:"Miami, FL",c:[25.774,-80.194]},{n:"Los Angeles, CA",c:[34.052,-118.244]},
  {n:"Philadelphia, PA",c:[39.952,-75.165]},{n:"New York, NY",c:[40.713,-74.006]},{n:"Detroit, MI",c:[42.331,-83.046]},
  {n:"Nashville, TN",c:[36.165,-86.784]},{n:"Memphis, TN",c:[35.149,-90.048]},{n:"New Orleans, LA",c:[29.951,-90.071]},
  {n:"Charlotte, NC",c:[35.227,-80.843]},{n:"Baltimore, MD",c:[39.290,-76.612]},{n:"Minneapolis, MN",c:[44.977,-93.265]},
  {n:"Seattle, WA",c:[47.606,-122.332]},{n:"Denver, CO",c:[39.739,-104.984]},{n:"Phoenix, AZ",c:[33.448,-112.074]},
  {n:"Kansas City, MO",c:[39.099,-94.578]},{n:"San Antonio, TX",c:[29.424,-98.494]},{n:"Jacksonville, FL",c:[30.332,-81.656]},
  {n:"Richmond, VA",c:[37.541,-77.433]},{n:"Birmingham, AL",c:[33.520,-86.803]},{n:"Boston, MA",c:[42.360,-71.058]},
  {n:"Columbus, OH",c:[39.961,-82.999]},{n:"San Diego, CA",c:[32.715,-117.157]},{n:"Portland, OR",c:[45.523,-122.676]},
  {n:"Cleveland, OH",c:[41.499,-81.695]},{n:"Cincinnati, OH",c:[39.103,-84.512]},{n:"St. Louis, MO",c:[38.627,-90.197]},
  {n:"Indianapolis, IN",c:[39.768,-86.158]},{n:"Louisville, KY",c:[38.253,-85.759]},{n:"Tampa, FL",c:[27.948,-82.458]},
  {n:"Orlando, FL",c:[28.538,-81.379]},{n:"Sacramento, CA",c:[38.576,-121.487]},{n:"Raleigh, NC",c:[35.779,-78.638]},
  {n:"Lexington, KY",c:[38.040,-84.503]},{n:"Gainesville, FL",c:[29.651,-82.325]},{n:"Baton Rouge, LA",c:[30.443,-91.187]},
  {n:"Mobile, AL",c:[30.696,-88.043]},{n:"Columbia, SC",c:[34.000,-81.035]},{n:"Jackson, MS",c:[32.299,-90.184]},
  {n:"Knoxville, TN",c:[35.961,-83.921]},{n:"Oklahoma City, OK",c:[35.467,-97.516]},{n:"Salt Lake City, UT",c:[40.760,-111.891]},
  {n:"Omaha, NE",c:[41.257,-95.995]},{n:"Pittsburgh, PA",c:[40.440,-79.995]},{n:"Greensboro, NC",c:[36.073,-79.792]},
  {n:"Newark, NJ",c:[40.735,-74.172]},{n:"Hartford, CT",c:[41.764,-72.685]},
];

// ── ATHLETE GENERATOR (deterministic, seeded RNG) ─────────────────────────────
const ATHLETES = (() => {
  let s = 0xABCDEF01 >>> 0;
  const r  = () => { s = (Math.imul(s,1664525)+1013904223)>>>0; return s/4294967296; };
  const p  = a  => a[(r()*a.length)|0];
  const rr = (lo,hi) => lo+r()*(hi-lo);
  const fx = (n,d=2) => parseFloat(n.toFixed(d));

  const FN=["Marcus","Devon","Tyler","Jamal","Caleb","Isaiah","Nathan","Elijah","Trey","Jordan","Darius","Kwame",
            "Brendan","Omari","Justin","Anthony","Cameron","Malik","Deontae","Reggie","Finley","Santos","Eric",
            "Langston","Andre","Terrell","Corey","Donovan","Miles","Deon","Xavier","Rashid","Aaron","Jalen","Kofi",
            "Tariq","Zion","Kendall","Darrell","Maurice","Curtis","Travis","Damian","Gerald","Victor","Frederick",
            "Leon","Roy","Derrick","Charles","Benjamin","Patrick","George","Thomas","James","David","Michael",
            "Robert","William","Joseph","Daniel","Steven","Kevin","Brian","Jason","Ryan","Timothy","Jeffrey",
            "Richard","Scott","Jonathan","Paul","Mark","Joshua","Gregory","Larry","Edward","Frank","Harold",
            "Raymond","Eugene","Carl","Phillip","Reginald","Kenneth","Donald","Douglas","Jerry","Dennis"];
  const LN=["Webb","Okafor","Osei","Rivers","Armstrong","Prince","Cruz","Frost","Daniels","Hayes","Holloway",
            "Powell","Asante","Stewart","Park","Morrow","Rhodes","Jefferson","Willis","Hart","Johnson","Medina",
            "Watkins","Grier","King","Brooks","Bell","Carr","Okonkwo","Speights","Tyson","Holt","Shore","Manning",
            "Reeves","Jackson","Williams","Brown","Jones","Davis","Miller","Wilson","Moore","Taylor","Anderson",
            "Thomas","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark","Rodriguez","Lewis","Lee",
            "Walker","Hall","Allen","Young","Hernandez","Wright","Lopez","Hill","Scott","Green","Adams","Baker",
            "Gonzalez","Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker",
            "Evans","Edwards","Collins","Morris","Rogers","Reed","Cook","Morgan","Bailey","Rivera","Cooper"];
  const HS=["Central High","Jefferson High","Lincoln High","Washington High","Roosevelt High","Kennedy High",
            "Franklin High","Madison High","Westlake High","Eastside High","Northgate High","Southview High",
            "Riverside High","Lakeside High","Hillcrest High","Valley View High","Pinecrest High","Summit High",
            "Heritage High","Freedom High","Liberty High","Pioneer High","Horizon High","Crestwood High",
            "Oakwood High","Maplewood High","Clearwater High","Brookfield High","Springfield High","Fairview High"];

  const GRP=[
    {ev:["100m","200m","4x100"],    tm:{100:[10.22,10.85],200:[20.90,22.40]}},
    {ev:["200m","400m","4x400"],    tm:{200:[20.90,22.20],400:[45.60,50.50]}},
    {ev:["400m","800m","4x400"],    tm:{400:[46.00,51.00],800:[105,120]}},
    {ev:["800m","1500m"],           tm:{800:[106,122],1500:[220,265]}},
    {ev:["1500m","Mile","5000m"],   tm:{1500:[222,268],Mile:[238,290],5000:[840,920]}},
    {ev:["5000m","10000m","XC"],    tm:{5000:[848,930],10000:[1752,1925]}},
    {ev:["110mH","400mH"],          tm:{110:[13.55,15.20],400:[50.0,56.0]}},
    {ev:["LJ","TJ","100m"],         tm:{100:[10.40,11.20]}},
    {ev:["HJ","PV"],                tm:{}},
    {ev:["SP","DT","JT"],           tm:{}},
    {ev:["60m","200m","4x400"],     tm:{60:[6.62,7.40],200:[21.10,22.80]}},
    {ev:["60mH","110mH"],           tm:{60:[7.52,8.50],110:[13.62,15.20]}},
    {ev:["Mile","3000m","5000m"],   tm:{Mile:[240,290],3000:[498,580]}},
    {ev:["SP","WT"],                tm:{}},
    {ev:["Pent","HJ","LJ"],        tm:{}},
    {ev:["60m","60mH","200m"],     tm:{60:[6.65,7.35],200:[21.20,22.70]}},
    {ev:["800m","Mile","1500m"],   tm:{800:[107,121],Mile:[239,289]}},
    {ev:["100m","200m","LJ"],      tm:{100:[10.25,10.90],200:[20.95,22.50]}},
    {ev:["400m","400mH"],          tm:{400:[46.50,51.50],400:[50.5,56.5]}},
    {ev:["60m","200m","400m"],     tm:{60:[6.68,7.42],200:[21.15,22.85],400:[46.0,51.0]}},
  ];

  return Array.from({length:200},(_,i)=>{
    const city=p(CITIES);
    const conf=p(ALL_CONFERENCES);
    const college=p(CONF_DATA[conf].colleges);
    const group=p(GRP);
    const hsYear=p([2019,2020,2021,2022,2023]);
    const collegeYear=p([1,2,3,4]);
    const hsTimes={}, collegeTimes={};
    Object.entries(group.tm).forEach(([ev,[lo,hi]])=>{
      const h=rr(lo+0.15,hi+0.25); const imp=rr(0.05,0.42);
      hsTimes[ev]=fx(h); collegeTimes[ev]=fx(h-imp);
    });
    return {id:i+1,name:`${p(FN)} ${p(LN)}`,hometown:city.n,hometownCoords:city.c,
      hsName:p(HS),college,conference:conf,events:group.ev,hsTimes,collegeTimes,
      hsYear,collegeYear,collegeCoords:CONF_DATA[conf].coords[college]};
  });
})();

const HS_YEARS=[2019,2020,2021,2022,2023];
const COLLEGE_YEARS=[1,2,3,4];

// ── UTILS ─────────────────────────────────────────────────────────────────────
const haversine=([a,b],[c,d])=>{const R=3958.8,dL=(c-a)*Math.PI/180,dl=(d-b)*Math.PI/180,x=Math.sin(dL/2)**2+Math.cos(a*Math.PI/180)*Math.cos(c*Math.PI/180)*Math.sin(dl/2)**2;return R*2*Math.atan2(Math.sqrt(x),Math.sqrt(1-x));};
const fmtDist=d=>`${Math.round(d).toLocaleString()} mi`;
const fmtTime=v=>{if(!v)return"—";if(v>200){const m=Math.floor(v/60);return`${m}:${(v%60).toFixed(2).padStart(5,"0")}`;}return v.toFixed(2);};
const distBucket=d=>d<100?"local":d<400?"regional":d<800?"far":"extreme";
const distColor=d=>d<100?T.green:d<400?T.yellow:d<800?T.orange:T.red;
const distLabel=d=>d<100?"Local (<100 mi)":d<400?"Regional (100–400 mi)":d<800?"Long Haul (400–800 mi)":"Cross-Country (800+ mi)";

// ── HEATMAP CANVAS ────────────────────────────────────────────────────────────
function drawHeatmap(canvas, athletes, projection) {
  if (!canvas || !projection || athletes.length === 0) return;
  const W = canvas.width, H = canvas.height;
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, W, H);

  const pts = athletes.map(a => projection([a.hometownCoords[1], a.hometownCoords[0]])).filter(Boolean);
  if (!pts.length) return;

  const density = new Float32Array(W * H);
  const R = 28, bw = R / 2.2;

  pts.forEach(([px, py]) => {
    const x0 = Math.max(0, (px-R)|0), x1 = Math.min(W-1, (px+R+1)|0);
    const y0 = Math.max(0, (py-R)|0), y1 = Math.min(H-1, (py+R+1)|0);
    for (let y=y0; y<=y1; y++) for (let x=x0; x<=x1; x++) {
      const dx=x-px, dy=y-py, d2=dx*dx+dy*dy;
      if (d2 < R*R) density[y*W+x] += Math.exp(-d2/(2*bw*bw));
    }
  });

  let mx=0; for (let i=0;i<density.length;i++) if(density[i]>mx) mx=density[i];
  if (!mx) return;

  const img = ctx.createImageData(W, H);
  for (let i=0; i<density.length; i++) {
    const t = Math.min(1, density[i]/mx);
    if (t < 0.008) continue;
    const idx = i*4;
    // Color ramp: SU blue → dark orange → primary orange → light orange → white
    if (t < 0.25) {
      const s=t/0.25; img.data[idx]=Math.round(s*215); img.data[idx+1]=Math.round(s*65); img.data[idx+2]=0; img.data[idx+3]=Math.round(s*180);
    } else if (t < 0.6) {
      const s=(t-0.25)/0.35; img.data[idx]=Math.round(215+s*32); img.data[idx+1]=Math.round(65+s*47); img.data[idx+2]=0; img.data[idx+3]=Math.round(180+s*50);
    } else if (t < 0.85) {
      const s=(t-0.6)/0.25; img.data[idx]=255; img.data[idx+1]=Math.round(112+s*30); img.data[idx+2]=0; img.data[idx+3]=Math.round(230+s*15);
    } else {
      const s=(t-0.85)/0.15; img.data[idx]=255; img.data[idx+1]=Math.round(142+s*113); img.data[idx+2]=Math.round(s*255); img.data[idx+3]=245;
    }
  }
  ctx.putImageData(img, 0, 0);
}

// ── US MAP ────────────────────────────────────────────────────────────────────
function USMap({athletes, onAthleteClick, selectedAthlete, highlightCollege, highlightHometown, mapMode, selectedStates}) {
  const svgRef=useRef(null), canvasRef=useRef(null), projRef=useRef(null);
  const [geo,setGeo]=useState(null), [tooltip,setTooltip]=useState(null);
  const FIPS_ABBR={"01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA","08":"CO","09":"CT","10":"DE","11":"DC","12":"FL","13":"GA","15":"HI","16":"ID","17":"IL","18":"IN","19":"IA","20":"KS","21":"KY","22":"LA","23":"ME","24":"MD","25":"MA","26":"MI","27":"MN","28":"MS","29":"MO","30":"MT","31":"NE","32":"NV","33":"NH","34":"NJ","35":"NM","36":"NY","37":"NC","38":"ND","39":"OH","40":"OK","41":"OR","42":"PA","44":"RI","45":"SC","46":"SD","47":"TN","48":"TX","49":"UT","50":"VT","51":"VA","53":"WA","54":"WV","55":"WI","56":"WY"};

  useEffect(()=>{
    const load=tj=>{fetch("https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json").then(r=>r.json()).then(us=>setGeo(tj.feature(us,us.objects.states)));};
    if(window.topojson){load(window.topojson);return;}
    const sc=document.createElement("script"); sc.src="https://cdn.jsdelivr.net/npm/topojson-client@3/dist/topojson-client.min.js"; sc.onload=()=>load(window.topojson); document.head.appendChild(sc);
  },[]);

  useEffect(()=>{
    if(!geo||!svgRef.current) return;
    const svg=d3.select(svgRef.current);
    const W=svgRef.current.clientWidth||960, H=svgRef.current.clientHeight||560;
    svg.selectAll("*").remove();
    const proj=d3.geoAlbersUsa().fitSize([W,H],geo);
    projRef.current=proj;
    const path=d3.geoPath().projection(proj);
    const g=svg.append("g");
    const px=([lat,lon])=>proj([lon,lat]);
    const hasStateFilter = selectedStates.length > 0;

    // Draw states — highlight selected ones
    g.selectAll("path.state").data(geo.features).join("path")
      .attr("class","state")
      .attr("d",path)
      .attr("fill",d=>{
        const abbr=FIPS_ABBR[String(d.id).padStart(2,"0")];
        if(!hasStateFilter) return "#E6E7EE";
        return selectedStates.includes(abbr)?"#FCC399":"#F1F2F5";
      })
      .attr("stroke",d=>{
        const abbr=FIPS_ABBR[String(d.id).padStart(2,"0")];
        return hasStateFilter&&selectedStates.includes(abbr)?T.orange:"#CCCFDD";
      })
      .attr("stroke-width",d=>{
        const abbr=FIPS_ABBR[String(d.id).padStart(2,"0")];
        return hasStateFilter&&selectedStates.includes(abbr)?2:0.8;
      })
      .style("cursor","default");

    if(mapMode==="heatmap") return;

    // Filter by selected states if any
    const stateFiltered = hasStateFilter
      ? athletes.filter(a=>selectedStates.includes(getState(a.hometown)))
      : athletes;

    const anyFocus=highlightCollege||highlightHometown;
    const active=anyFocus?stateFiltered.filter(a=>(highlightCollege?a.college===highlightCollege:true)&&(highlightHometown?a.hometown===highlightHometown:true)):stateFiltered;
    const dimmed=athletes.filter(a=>!stateFiltered.includes(a));

    if(mapMode==="flows"){
      dimmed.forEach(a=>{
        const h=px(a.hometownCoords),c=px(a.collegeCoords); if(!h||!c) return;
        const dx=c[0]-h[0],dy=c[1]-h[1],dr=Math.sqrt(dx*dx+dy*dy)*0.55;
        g.append("path").attr("d",`M${h[0]},${h[1]} A${dr},${dr} 0 0,1 ${c[0]},${c[1]}`).attr("fill","none").attr("stroke","rgba(0,0,0,0.04)").attr("stroke-width",0.8);
      });
      active.forEach(a=>{
        const h=px(a.hometownCoords),c=px(a.collegeCoords); if(!h||!c) return;
        const dist=haversine(a.hometownCoords,a.collegeCoords), col=distColor(dist), isSel=selectedAthlete?.id===a.id;
        const dx=c[0]-h[0],dy=c[1]-h[1],dr=Math.sqrt(dx*dx+dy*dy)*0.55;
        const arc=`M${h[0]},${h[1]} A${dr},${dr} 0 0,1 ${c[0]},${c[1]}`;
        if(isSel) g.append("path").attr("d",arc).attr("fill","none").attr("stroke",`${T.orange}55`).attr("stroke-width",8);
        g.append("path").attr("d",arc).attr("fill","none").attr("stroke",isSel?T.orange:col).attr("stroke-width",isSel?2.5:1.6).attr("stroke-opacity",isSel?1:0.82).style("cursor","pointer")
          .on("mouseover",function(ev){d3.select(this).attr("stroke-width",3).attr("stroke-opacity",1);setTooltip({x:ev.offsetX,y:ev.offsetY,a,dist:Math.round(dist)});})
          .on("mouseout",function(){d3.select(this).attr("stroke-width",isSel?2.5:1.6).attr("stroke-opacity",isSel?1:0.82);setTooltip(null);})
          .on("click",ev=>{ev.stopPropagation();onAthleteClick(a);});
        const sz=isSel?5.5:3.5;
        g.append("circle").attr("cx",h[0]).attr("cy",h[1]).attr("r",sz).attr("fill",col).attr("stroke","#FFFFFF").attr("stroke-width",1.5).attr("opacity",0.95);
        const s=isSel?7:5;
        g.append("rect").attr("x",c[0]-s/2).attr("y",c[1]-s/2).attr("width",s).attr("height",s).attr("fill",col).attr("stroke","#FFFFFF").attr("stroke-width",1.5).attr("opacity",0.95).attr("rx",1);
      });
    } else {
      athletes.forEach(a=>{
        const coord=mapMode==="college"?a.collegeCoords:a.hometownCoords;
        const p=px(coord); if(!p) return;
        const isSel=selectedAthlete?.id===a.id, isAct=active.includes(a), rr=isSel?9:5.5;
        if(isSel) g.append("circle").attr("cx",p[0]).attr("cy",p[1]).attr("r",16).attr("fill",`${T.orange}20`).attr("stroke","none");
        g.append("circle").attr("cx",p[0]).attr("cy",p[1]).attr("r",rr)
          .attr("fill",isSel?T.orange:isAct?T.orangeL:"rgba(200,120,60,0.2)")
          .attr("stroke",isSel?T.white:"#FFFFFF").attr("stroke-width",isSel?2:1).attr("opacity",isAct?0.9:0.25)
          .style("cursor","pointer")
          .on("mouseover",function(ev){d3.select(this).attr("r",rr+3);setTooltip({x:ev.offsetX,y:ev.offsetY,a,dist:Math.round(haversine(a.hometownCoords,a.collegeCoords))});})
          .on("mouseout",function(){d3.select(this).attr("r",rr);setTooltip(null);})
          .on("click",()=>onAthleteClick(a));
      });
    }
  },[geo,athletes,selectedAthlete,highlightCollege,highlightHometown,mapMode,selectedStates]);

  // Draw heatmap canvas
  useEffect(()=>{
    if(mapMode!=="heatmap"||!canvasRef.current||!projRef.current) return;
    drawHeatmap(canvasRef.current, athletes, projRef.current);
  },[mapMode, athletes, geo]);

  return(
    <div style={{position:"relative",width:"100%",height:"100%"}}>
      <svg ref={svgRef} style={{width:"100%",height:"100%",display:"block"}}/>
      <canvas ref={canvasRef} style={{position:"absolute",top:0,left:0,pointerEvents:"none",opacity:mapMode==="heatmap"?1:0,transition:"opacity 0.3s",width:"100%",height:"100%"}}
        width={svgRef.current?.clientWidth||960} height={svgRef.current?.clientHeight||560}/>
      {tooltip&&(
        <div style={{position:"absolute",left:tooltip.x+14,top:tooltip.y-8,background:"#FFFFFF",border:`1px solid ${T.orange}`,borderRadius:9,padding:"10px 14px",pointerEvents:"none",zIndex:100,boxShadow:`0 6px 28px rgba(247,105,0,0.18)`,minWidth:180}}>
          <div style={{color:T.orange,fontWeight:800,fontSize:14,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>{tooltip.a.name}</div>
          <div style={{color:T.offWhite,fontSize:11,marginTop:2}}>{tooltip.a.hometown}</div>
          <div style={{color:T.muted,fontSize:11}}>→ {tooltip.a.college} ({tooltip.a.conference})</div>
          <div style={{display:"flex",alignItems:"center",gap:8,marginTop:6}}>
            <div style={{width:8,height:8,borderRadius:"50%",background:distColor(tooltip.dist)}}/>
            <span style={{color:distColor(tooltip.dist),fontSize:13,fontWeight:800,fontFamily:"monospace"}}>{fmtDist(tooltip.dist)}</span>
          </div>
          <div style={{color:T.dim,fontSize:10,marginTop:3}}>{tooltip.a.events.slice(0,3).join(" · ")}</div>
        </div>
      )}
    </div>
  );
}

// ── UI ATOMS ──────────────────────────────────────────────────────────────────
const Chip=({label,active,onClick,color})=>(
  <button onClick={onClick} style={{background:active?`rgba(${color||"247,105,0"},0.18)`:"rgba(255,255,255,0.03)",border:`1px solid ${active?`rgba(${color||"247,105,0"},0.9)`:T.border}`,color:active?`rgb(${color||"247,105,0"})`:T.muted,borderRadius:4,padding:"3px 8px",fontSize:11,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,cursor:"pointer",transition:"all 0.12s",whiteSpace:"nowrap"}}>{label}</button>
);

const Sel=({value,onChange,options,placeholder})=>(
  <select value={value} onChange={e=>onChange(e.target.value)} style={{background:T.bgCard,border:`1px solid ${T.border}`,color:value?T.offWhite:T.muted,borderRadius:6,padding:"5px 8px",fontSize:12,width:"100%",cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,outline:"none"}}>
    <option value="">{placeholder}</option>
    {options.map(o=><option key={o.value||o} value={o.value||o}>{o.label||o}</option>)}
  </select>
);

const StatCard=({label,value,color})=>(
  <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:7,padding:"8px 10px"}}>
    <div style={{color:T.muted,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:2,textTransform:"uppercase"}}>{label}</div>
    <div style={{color:color||T.orange,fontSize:16,fontWeight:800,fontFamily:"'Barlow Condensed',sans-serif",marginTop:2}}>{value}</div>
  </div>
);

const SectionHead=({children})=>(
  <div style={{display:"flex",alignItems:"center",gap:8,margin:"14px 0 8px"}}>
    <div style={{flex:1,height:1,background:`${T.orange}28`}}/>
    <span style={{color:T.orange,fontSize:10,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:2,textTransform:"uppercase"}}>{children}</span>
    <div style={{flex:1,height:1,background:`${T.orange}28`}}/>
  </div>
);

function DistBar({athletes}){
  const total=athletes.length; if(!total) return null;
  const b={local:0,regional:0,far:0,extreme:0};
  athletes.forEach(a=>{b[distBucket(haversine(a.hometownCoords,a.collegeCoords))]++;});
  const cols={local:T.green,regional:T.yellow,far:T.orange,extreme:T.red};
  const lbl={local:"<100",regional:"100–400",far:"400–800",extreme:"800+"};
  return(
    <div>
      <div style={{display:"flex",height:6,borderRadius:3,overflow:"hidden",gap:1,marginBottom:5}}>
        {Object.entries(b).filter(([,n])=>n>0).map(([k,n])=><div key={k} style={{flex:n,background:cols[k],opacity:0.85,minWidth:2}}/>)}
      </div>
      <div style={{display:"flex",gap:8,flexWrap:"wrap"}}>
        {Object.entries(b).map(([k,n])=>(
          <div key={k} style={{display:"flex",gap:4,alignItems:"center"}}>
            <div style={{width:6,height:6,borderRadius:"50%",background:cols[k]}}/>
            <span style={{color:T.muted,fontSize:9}}>{lbl[k]}</span>
            <span style={{color:T.offWhite,fontSize:9,fontWeight:700}}>{n}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── STATE MULTI-SELECT DROPDOWN ───────────────────────────────────────────────
const ALL_STATE_ABBRS = Object.keys(STATE_NAMES).sort((a,b)=>STATE_NAMES[a].localeCompare(STATE_NAMES[b]));

function StateFilterDropdown({selectedStates, onChange}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const dropRef = useRef(null);

  // Close on outside click
  useEffect(()=>{
    const handler = e => { if(dropRef.current && !dropRef.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", handler);
    return ()=>document.removeEventListener("mousedown", handler);
  },[]);

  const filtered = ALL_STATE_ABBRS.filter(abbr=>
    STATE_NAMES[abbr].toLowerCase().includes(search.toLowerCase()) ||
    abbr.toLowerCase().includes(search.toLowerCase())
  );

  const toggle = abbr => {
    onChange(selectedStates.includes(abbr)
      ? selectedStates.filter(s=>s!==abbr)
      : [...selectedStates, abbr]
    );
  };

  const count = selectedStates.length;

  return(
    <div ref={dropRef} style={{position:"relative",marginBottom:9}}>
      <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:5}}>
        Origin State {count>0&&<span style={{color:T.orange,fontWeight:800}}>({count} selected)</span>}
      </div>

      {/* Trigger button */}
      <button onClick={()=>setOpen(o=>!o)} style={{
        width:"100%",display:"flex",justifyContent:"space-between",alignItems:"center",
        background:T.bg,border:`1px solid ${count>0?T.orange:T.border}`,borderRadius:6,
        padding:"5px 8px",cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",
        fontSize:12,color:count>0?T.orange:T.muted,letterSpacing:1,
      }}>
        <span>{count===0?"All States":count===1?STATE_NAMES[selectedStates[0]]:`${count} States`}</span>
        <span style={{fontSize:9,opacity:0.6}}>{open?"▲":"▼"}</span>
      </button>

      {/* Dropdown panel */}
      {open && (
        <div style={{
          position:"absolute",top:"100%",left:0,right:0,zIndex:200,
          background:T.bg,border:`1px solid ${T.border}`,borderRadius:7,
          boxShadow:"0 8px 24px rgba(0,0,0,0.12)",marginTop:3,
          display:"flex",flexDirection:"column",maxHeight:280,overflow:"hidden",
        }}>
          {/* Search */}
          <div style={{padding:"7px 8px",borderBottom:`1px solid ${T.border}`,flexShrink:0}}>
            <input
              autoFocus
              value={search}
              onChange={e=>setSearch(e.target.value)}
              placeholder="Search states..."
              style={{width:"100%",background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:5,
                padding:"4px 8px",fontSize:11,color:T.offWhite,fontFamily:"'Barlow',sans-serif",outline:"none"}}
            />
          </div>

          {/* Select All / None */}
          <div style={{display:"flex",gap:6,padding:"5px 8px",borderBottom:`1px solid ${T.border}`,flexShrink:0}}>
            <button onClick={()=>onChange(ALL_STATE_ABBRS)} style={{flex:1,background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:4,padding:"3px 0",fontSize:10,color:T.orange,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>SELECT ALL</button>
            <button onClick={()=>onChange([])} style={{flex:1,background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:4,padding:"3px 0",fontSize:10,color:T.muted,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>SELECT NONE</button>
          </div>

          {/* Checkbox list */}
          <div style={{overflowY:"auto",flex:1}}>
            {filtered.length===0?(
              <div style={{padding:"12px 8px",color:T.dim,fontSize:11,textAlign:"center"}}>No states found</div>
            ):filtered.map(abbr=>{
              const checked = selectedStates.includes(abbr);
              return(
                <label key={abbr} style={{display:"flex",alignItems:"center",gap:8,padding:"5px 10px",cursor:"pointer",background:checked?`${T.orange}08`:"transparent",borderBottom:`1px solid ${T.border}22`}}>
                  <div style={{
                    width:14,height:14,borderRadius:3,flexShrink:0,
                    background:checked?T.orange:T.bg,
                    border:`2px solid ${checked?T.orange:T.border}`,
                    display:"flex",alignItems:"center",justifyContent:"center",
                    transition:"all 0.1s",
                  }}>
                    {checked&&<span style={{color:"#fff",fontSize:9,lineHeight:1,fontWeight:900}}>✓</span>}
                  </div>
                  <input type="checkbox" checked={checked} onChange={()=>toggle(abbr)} style={{display:"none"}}/>
                  <span style={{color:checked?T.orange:T.offWhite,fontSize:12,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:0.5,fontWeight:checked?700:400}}>{STATE_NAMES[abbr]}</span>
                  <span style={{color:T.dim,fontSize:10,fontFamily:"monospace",marginLeft:"auto"}}>{abbr}</span>
                </label>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

// ── FILTER CONTROLS ───────────────────────────────────────────────────────────
function FilterControls({filters, setFilters, showSeason=false, selectedStates=[], onStatesChange=()=>{}}) {
  const confColleges = getConfColleges(filters.conference);
  const season = filters.season || "all";
  const visibleEvents = EVENTS_CFG.filter(e => season==="all" || e.season==="both" || e.season===season);

  const toggleEvent = ev => setFilters(f=>({...f,events:f.events.includes(ev)?f.events.filter(e=>e!==ev):[...f.events,ev]}));
  const setConf = v => setFilters(f=>({...f,conference:v,college:""})); // reset college on conf change

  return(
    <div>
      {showSeason && (
        <div style={{marginBottom:11}}>
          <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Season</div>
          <div style={{display:"flex",gap:3}}>
            {[["all","All"],["indoor","Indoor"],["outdoor","Outdoor"]].map(([v,l])=>(
              <Chip key={v} label={l} active={season===v} onClick={()=>setFilters(f=>({...f,season:v,events:[]}))} color="247,105,0"/>
            ))}
          </div>
        </div>
      )}
      <div style={{marginBottom:11}}>
        <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Events</div>
        <div style={{display:"flex",flexWrap:"wrap",gap:3}}>
          {visibleEvents.map(e=>{
            const isIndoor=e.season==="indoor", isOutdoor=e.season==="outdoor";
            const col=isIndoor?"43,114,215":isOutdoor?"32,50,153":"247,105,0";
            return <Chip key={e.id} label={e.id} active={filters.events.includes(e.id)} onClick={()=>toggleEvent(e.id)} color={col}/>;
          })}
        </div>
        <div style={{display:"flex",gap:10,marginTop:5}}>
          {[["247,105,0","Both"],["43,114,215","Indoor"],["32,50,153","Outdoor"]].map(([c,l])=>(
            <div key={l} style={{display:"flex",gap:4,alignItems:"center"}}>
              <div style={{width:6,height:6,borderRadius:"50%",background:`rgb(${c})`}}/>
              <span style={{color:T.dim,fontSize:9}}>{l}</span>
            </div>
          ))}
        </div>
      </div>
      <div style={{marginBottom:9}}>
        <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:5}}>Conference</div>
        <Sel value={filters.conference} onChange={setConf} options={ALL_CONFERENCES} placeholder="All Conferences"/>
      </div>
      <div style={{marginBottom:9}}>
        <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:5}}>College{filters.conference?` (${filters.conference})`:""}</div>
        <Sel value={filters.college} onChange={v=>setFilters(f=>({...f,college:v}))} options={confColleges.sort()} placeholder={filters.conference?`All ${filters.conference}`:"All Colleges"}/>
      </div>
      <div style={{marginBottom:9}}>
        <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:5}}>HS Grad Year</div>
        <Sel value={filters.hsYear||""} onChange={v=>setFilters(f=>({...f,hsYear:v}))} options={HS_YEARS.map(y=>({value:y,label:`Class of ${y}`}))} placeholder="All Years"/>
      </div>
      <div style={{marginBottom:9}}>
        <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>College Year</div>
        <div style={{display:"flex",gap:3}}>
          {COLLEGE_YEARS.map(y=><Chip key={y} label={`Y${y}`} active={filters.collegeYear===String(y)} onClick={()=>setFilters(f=>({...f,collegeYear:f.collegeYear===String(y)?"":String(y)}))}/>)}
        </div>
      </div>
      <StateFilterDropdown selectedStates={selectedStates} onChange={onStatesChange}/>
    </div>
  );
}

function applyFilters(athletes, filters, search="") {
  return athletes.filter(a=>{
    if(filters.season && filters.season !== "all") {
      const eventsInSeason = EVENTS_CFG.filter(e=>e.season==="both"||e.season===filters.season).map(e=>e.id);
      if(!a.events.some(e=>eventsInSeason.includes(e))) return false;
    }
    if(filters.events?.length>0 && !filters.events.some(e=>a.events.includes(e))) return false;
    if(filters.conference && a.conference!==filters.conference) return false;
    if(filters.college && a.college!==filters.college) return false;
    if(filters.hsYear && a.hsYear!==parseInt(filters.hsYear)) return false;
    if(filters.collegeYear && a.collegeYear!==parseInt(filters.collegeYear)) return false;
    if(search){const q=search.toLowerCase();if(!a.name.toLowerCase().includes(q)&&!a.college.toLowerCase().includes(q)&&!a.hometown.toLowerCase().includes(q)) return false;}
    return true;
  });
}

// ── HEATMAP PANEL ─────────────────────────────────────────────────────────────
// State abbreviation → full name map
const STATE_NAMES = {
  AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",
  CO:"Colorado",CT:"Connecticut",DE:"Delaware",FL:"Florida",GA:"Georgia",
  HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",
  KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",
  MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",
  NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",
  NY:"New York",NC:"North Carolina",ND:"North Dakota",OH:"Ohio",OK:"Oklahoma",
  OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"South Carolina",
  SD:"South Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",
  VA:"Virginia",WA:"Washington",WV:"West Virginia",WI:"Wisconsin",WY:"Wyoming",
  DC:"Washington D.C.",
};

// Extract state abbreviation from "City, ST" string
const getState = (hometown) => {
  const parts = hometown.split(", ");
  return parts.length >= 2 ? parts[parts.length - 1].trim() : null;
};

function HeatmapPanel({athletes}) {
  const [rankTab, setRankTab] = useState("cities");

  const topCities = useMemo(()=>{
    const map={};
    athletes.forEach(a=>{if(!map[a.hometown])map[a.hometown]={city:a.hometown,count:0};map[a.hometown].count++;});
    return Object.values(map).sort((a,b)=>b.count-a.count).slice(0,15);
  },[athletes]);

  const topStates = useMemo(()=>{
    const map={};
    athletes.forEach(a=>{
      const st = getState(a.hometown);
      if(!st) return;
      if(!map[st]) map[st]={abbr:st,name:STATE_NAMES[st]||st,count:0,cities:new Set()};
      map[st].count++;
      map[st].cities.add(a.hometown);
    });
    return Object.values(map)
      .map(s=>({...s,cityCount:s.cities.size}))
      .sort((a,b)=>b.count-a.count)
      .slice(0,15);
  },[athletes]);

  const uniqueStateCount = useMemo(()=>{
    const s=new Set(); athletes.forEach(a=>{const st=getState(a.hometown);if(st)s.add(st);}); return s.size;
  },[athletes]);

  return(
    <div style={{padding:"14px",height:"100%",overflowY:"auto"}}>
      {/* Header info */}
      <div style={{marginBottom:12}}>
        <div style={{color:T.orange,fontFamily:"'Barlow Condensed',sans-serif",fontSize:12,letterSpacing:2,textTransform:"uppercase",marginBottom:6}}>Hometown Density Map</div>
        <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"10px 12px",marginBottom:10}}>
          <div style={{color:T.muted,fontSize:11,lineHeight:1.6}}>
            The heatmap reflects your <span style={{color:T.orange,fontWeight:700}}>active filters</span> in the left panel. Adjust events, conference, college, year, or season to update the density view.
          </div>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:6}}>
          <StatCard label="Athletes" value={athletes.length} color={T.orange}/>
          <StatCard label="Cities" value={topCities.length} color={T.blueL}/>
          <StatCard label="States" value={uniqueStateCount} color={T.blueM}/>
        </div>
      </div>

      {/* Color scale */}
      <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"10px 12px",marginBottom:14}}>
        <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:7}}>Density Scale</div>
        <div style={{display:"flex",height:10,borderRadius:4,overflow:"hidden",marginBottom:5}}>
          {["#000E54","#D74100","#F76900","#FF8E00","#FF431B","#FCC399","#FFFFFF"].map((c,i)=>(
            <div key={i} style={{flex:1,background:c}}/>
          ))}
        </div>
        <div style={{display:"flex",justifyContent:"space-between"}}>
          <span style={{color:T.dim,fontSize:9}}>Low density</span>
          <span style={{color:T.dim,fontSize:9}}>High density</span>
        </div>
      </div>

      {/* Cities / States tab toggle */}
      <div style={{display:"flex",background:T.bg,borderRadius:7,border:`1px solid ${T.border}`,overflow:"hidden",marginBottom:12}}>
        {[["cities","🏙 Top Cities"],["states","🗺 Top States"]].map(([tab,lbl])=>(
          <button key={tab} onClick={()=>setRankTab(tab)} style={{flex:1,padding:"7px 6px",background:rankTab===tab?T.orange:"transparent",border:"none",color:rankTab===tab?T.white:T.muted,fontSize:11,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",transition:"all 0.15s",fontWeight:rankTab===tab?700:400}}>{lbl}</button>
        ))}
      </div>

      {/* Top Cities */}
      {rankTab === "cities" && (
        <>
          <div style={{display:"grid",gridTemplateColumns:"auto 1fr auto",gap:"0 10px",marginBottom:4,paddingBottom:5,borderBottom:`1px solid ${T.border}`}}>
            <span style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase"}}>#</span>
            <span style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase"}}>City</span>
            <span style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",textAlign:"right"}}>Athletes</span>
          </div>
          {topCities.length === 0 ? (
            <div style={{color:T.dim,fontSize:11,textAlign:"center",padding:"20px 0"}}>No athletes match current filters</div>
          ) : topCities.map((c,i)=>(
            <div key={c.city} style={{display:"grid",gridTemplateColumns:"auto 1fr auto",gap:"0 10px",alignItems:"center",padding:"6px 0",borderBottom:`1px solid ${T.border}44`}}>
              <span style={{color:i<3?T.orange:T.dim,fontSize:11,fontFamily:"monospace",minWidth:20,fontWeight:i<3?800:400}}>#{i+1}</span>
              <div>
                <div style={{color:T.offWhite,fontSize:12}}>{c.city}</div>
                <div style={{height:3,borderRadius:2,background:T.orange,opacity:0.5,marginTop:3,width:Math.max(6,Math.round(c.count/topCities[0].count*100))+'%'}}/>
              </div>
              <span style={{color:i<3?T.orange:T.offWhite,fontSize:13,fontWeight:800,fontFamily:"monospace",textAlign:"right"}}>{c.count}</span>
            </div>
          ))}
        </>
      )}

      {/* Top States */}
      {rankTab === "states" && (
        <>
          <div style={{display:"grid",gridTemplateColumns:"auto 1fr auto auto",gap:"0 8px",marginBottom:4,paddingBottom:5,borderBottom:`1px solid ${T.border}`}}>
            <span style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase"}}>#</span>
            <span style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase"}}>State</span>
            <span style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",textAlign:"center"}}>Cities</span>
            <span style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",textAlign:"right"}}>Athletes</span>
          </div>
          {topStates.length === 0 ? (
            <div style={{color:T.dim,fontSize:11,textAlign:"center",padding:"20px 0"}}>No athletes match current filters</div>
          ) : topStates.map((s,i)=>(
            <div key={s.abbr} style={{display:"grid",gridTemplateColumns:"auto 1fr auto auto",gap:"0 8px",alignItems:"center",padding:"6px 0",borderBottom:`1px solid ${T.border}44`}}>
              <span style={{color:i<3?T.orange:T.dim,fontSize:11,fontFamily:"monospace",minWidth:20,fontWeight:i<3?800:400}}>#{i+1}</span>
              <div>
                <div style={{display:"flex",gap:6,alignItems:"center"}}>
                  <span style={{color:T.white,fontSize:10,fontWeight:800,fontFamily:"'Barlow Condensed',sans-serif",background:i<3?T.orange:T.border,borderRadius:3,padding:"1px 5px",letterSpacing:1}}>{s.abbr}</span>
                  <span style={{color:T.offWhite,fontSize:12}}>{s.name}</span>
                </div>
                <div style={{height:3,borderRadius:2,background:T.orange,opacity:0.5,marginTop:3,width:Math.max(6,Math.round(s.count/topStates[0].count*100))+'%'}}/>
              </div>
              <span style={{color:T.muted,fontSize:10,textAlign:"center",fontFamily:"monospace"}}>{s.cityCount}</span>
              <span style={{color:i<3?T.orange:T.offWhite,fontSize:13,fontWeight:800,fontFamily:"monospace",textAlign:"right"}}>{s.count}</span>
            </div>
          ))}
        </>
      )}
    </div>
  );
}

// ── COLLEGE PULL PANEL ────────────────────────────────────────────────────────
function CollegePullPanel({athletes,focusedCollege,onFocusCollege}){
  const stats=useMemo(()=>{
    const map={};
    athletes.forEach(a=>{
      if(!map[a.college])map[a.college]={college:a.college,conference:a.conference,list:[],hometowns:new Set()};
      map[a.college].list.push({...a,dist:haversine(a.hometownCoords,a.collegeCoords)});
      map[a.college].hometowns.add(a.hometown);
    });
    return Object.values(map).map(c=>({...c,count:c.list.length,avgDist:c.list.reduce((s,a)=>s+a.dist,0)/c.list.length,maxDist:Math.max(...c.list.map(a=>a.dist)),hometownCount:c.hometowns.size})).sort((a,b)=>b.count-a.count);
  },[athletes]);
  const focused=stats.find(c=>c.college===focusedCollege);

  return(
    <div style={{height:"100%",overflowY:"auto",padding:"14px"}}>
      <SectionHead>College Pull Analysis</SectionHead>
      {!focusedCollege?(
        <>
          <div style={{color:T.muted,fontSize:11,marginBottom:12,lineHeight:1.5}}>Which schools pull from where & how far?</div>
          {stats.map(c=>(
            <button key={c.college} onClick={()=>onFocusCollege(c.college)} style={{display:"block",width:"100%",textAlign:"left",background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"9px 11px",marginBottom:5,cursor:"pointer",transition:"border-color 0.12s"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
                <div><div style={{color:T.offWhite,fontWeight:700,fontSize:13,fontFamily:"'Barlow Condensed',sans-serif"}}>{c.college}</div><div style={{color:T.dim,fontSize:10,marginTop:1}}>{c.conference} · {c.hometownCount} cities</div></div>
                <div style={{textAlign:"right"}}><div style={{color:T.orange,fontWeight:800,fontSize:14,fontFamily:"'Barlow Condensed',sans-serif"}}>{c.count}</div><div style={{color:distColor(c.avgDist),fontSize:10}}>avg {fmtDist(c.avgDist)}</div></div>
              </div>
              <div style={{marginTop:7}}><DistBar athletes={c.list}/></div>
            </button>
          ))}
        </>
      ):focused?(
        <>
          <button onClick={()=>onFocusCollege("")} style={{background:"none",border:`1px solid ${T.border}`,color:T.orange,borderRadius:5,padding:"4px 10px",fontSize:11,cursor:"pointer",marginBottom:12,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>← All Colleges</button>
          <div style={{background:T.bgCard,border:`1px solid ${T.orange}44`,borderRadius:9,padding:"12px",marginBottom:12}}>
            <div style={{color:T.orange,fontFamily:"'Barlow Condensed',sans-serif",fontSize:19,fontWeight:900,letterSpacing:1}}>{focused.college}</div>
            <div style={{color:T.muted,fontSize:11,marginBottom:10}}>{focused.conference}</div>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6,marginBottom:10}}>
              <StatCard label="Athletes" value={focused.count}/><StatCard label="Hometowns" value={focused.hometownCount} color={T.offWhite}/>
              <StatCard label="Avg Distance" value={fmtDist(focused.avgDist)} color={distColor(focused.avgDist)}/><StatCard label="Farthest" value={fmtDist(focused.maxDist)} color={T.red}/>
            </div>
            <DistBar athletes={focused.list}/>
          </div>
          <SectionHead>Athletes & Origins</SectionHead>
          {focused.list.sort((a,b)=>b.dist-a.dist).map(a=>(
            <div key={a.id} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"7px 0",borderBottom:`1px solid ${T.border}`}}>
              <div><div style={{color:T.offWhite,fontSize:12,fontWeight:600}}>{a.name}</div><div style={{color:T.muted,fontSize:10,marginTop:1}}>📍 {a.hometown}</div></div>
              <div style={{color:distColor(a.dist),fontSize:12,fontWeight:800,fontFamily:"monospace"}}>{fmtDist(a.dist)}</div>
            </div>
          ))}
        </>
      ):null}
    </div>
  );
}

// ── HOMETOWN PANEL ────────────────────────────────────────────────────────────
function HometownPanel({athletes,focusedHometown,onFocusHometown}){
  const stats=useMemo(()=>{
    const map={};
    athletes.forEach(a=>{
      if(!map[a.hometown])map[a.hometown]={hometown:a.hometown,list:[],colleges:new Set()};
      map[a.hometown].list.push({...a,dist:haversine(a.hometownCoords,a.collegeCoords)});
      map[a.hometown].colleges.add(a.college);
    });
    return Object.values(map).map(h=>({...h,count:h.list.length,avgDist:h.list.reduce((s,a)=>s+a.dist,0)/h.list.length,maxDist:Math.max(...h.list.map(a=>a.dist)),collegeCount:h.colleges.size})).sort((a,b)=>b.count-a.count);
  },[athletes]);
  const focused=stats.find(h=>h.hometown===focusedHometown);

  return(
    <div style={{height:"100%",overflowY:"auto",padding:"14px"}}>
      <SectionHead>Hometown Destinations</SectionHead>
      {!focusedHometown?(
        <>
          <div style={{color:T.muted,fontSize:11,marginBottom:12,lineHeight:1.5}}>Where do athletes from each city go?</div>
          {stats.map(h=>(
            <button key={h.hometown} onClick={()=>onFocusHometown(h.hometown)} style={{display:"block",width:"100%",textAlign:"left",background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"9px 11px",marginBottom:5,cursor:"pointer"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
                <div><div style={{color:T.offWhite,fontWeight:700,fontSize:13,fontFamily:"'Barlow Condensed',sans-serif"}}>{h.hometown}</div><div style={{color:T.dim,fontSize:10,marginTop:1}}>{h.collegeCount} colleges</div></div>
                <div style={{textAlign:"right"}}><div style={{color:T.orange,fontWeight:800,fontSize:14,fontFamily:"'Barlow Condensed',sans-serif"}}>{h.count}</div><div style={{color:distColor(h.avgDist),fontSize:10}}>avg {fmtDist(h.avgDist)}</div></div>
              </div>
              <div style={{marginTop:7}}><DistBar athletes={h.list}/></div>
            </button>
          ))}
        </>
      ):focused?(
        <>
          <button onClick={()=>onFocusHometown("")} style={{background:"none",border:`1px solid ${T.border}`,color:T.orange,borderRadius:5,padding:"4px 10px",fontSize:11,cursor:"pointer",marginBottom:12,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>← All Hometowns</button>
          <div style={{background:T.bgCard,border:`1px solid ${T.orange}44`,borderRadius:9,padding:"12px",marginBottom:12}}>
            <div style={{color:T.orange,fontFamily:"'Barlow Condensed',sans-serif",fontSize:19,fontWeight:900,letterSpacing:1}}>📍 {focused.hometown}</div>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6,marginTop:10,marginBottom:10}}>
              <StatCard label="Athletes" value={focused.count}/><StatCard label="Colleges" value={focused.collegeCount} color={T.offWhite}/>
              <StatCard label="Avg Distance" value={fmtDist(focused.avgDist)} color={distColor(focused.avgDist)}/><StatCard label="Farthest" value={fmtDist(focused.maxDist)} color={T.red}/>
            </div>
            <DistBar athletes={focused.list}/>
          </div>
          <SectionHead>Where Athletes Went</SectionHead>
          {focused.list.sort((a,b)=>b.dist-a.dist).map(a=>(
            <div key={a.id} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"7px 0",borderBottom:`1px solid ${T.border}`}}>
              <div><div style={{color:T.offWhite,fontSize:12,fontWeight:600}}>{a.name}</div><div style={{color:T.muted,fontSize:10,marginTop:1}}>🎓 {a.college} · {a.conference}</div></div>
              <div style={{color:distColor(a.dist),fontSize:12,fontWeight:800,fontFamily:"monospace"}}>{fmtDist(a.dist)}</div>
            </div>
          ))}
        </>
      ):null}
    </div>
  );
}

// ── PERFORMANCE YEAR HELPERS ──────────────────────────────────────────────────
// Generates plausible per-year times deterministically from stored best mark
function genHSTimes(athlete, eventKey) {
  const best = athlete.hsTimes[eventKey];
  if (best == null) return [];
  // seed per athlete+event for stable variance
  const seed = (athlete.id * 31 + (eventKey.charCodeAt(0) + (eventKey.charCodeAt(1)||7)) * 7) % 100;
  const totalPct = 0.022 + seed * 0.0012; // 2.2%–14.4% slower freshman vs senior
  const labels = ["Fr","So","Jr","Sr"];
  const gradients = [1, 0.60, 0.22, 0]; // fraction of improvement remaining each year
  return labels.map((lbl, i) => ({
    label: lbl,
    year: athlete.hsYear - 3 + i,
    time: parseFloat((best * (1 + totalPct * gradients[i])).toFixed(2)),
    isBest: i === 3,
  }));
}

function genCollegeTimes(athlete, eventKey) {
  const colBest = athlete.collegeTimes[eventKey];
  if (colBest == null) return [];
  const hsBest = athlete.hsTimes[eventKey];
  const seed = (athlete.id * 17 + (eventKey.charCodeAt(0) + (eventKey.charCodeAt(1)||11)) * 13) % 100;
  const totalImprove = hsBest != null
    ? (hsBest - colBest) * 1.05  // extend a hair past stored gap
    : colBest * (0.018 + seed * 0.0006);
  const gradients = [0, 0.35, 0.70, 1.0]; // fraction of improvement gained each year
  const maxYear = athlete.collegeYear;
  return [1,2,3,4].map(y => ({
    label: `Y${y}`,
    year: athlete.hsYear + y,
    time: parseFloat((colBest + totalImprove * (1 - gradients[y-1])).toFixed(2)),
    isBest: y === maxYear,
    isCurrent: y === maxYear,
    isFuture: y > maxYear,
  }));
}

// ── ATHLETE DETAIL ────────────────────────────────────────────────────────────
function AthleteDetail({athlete,onClose}){
  const [perfTab, setPerfTab] = useState("hs");

  if(!athlete) return(
    <div style={{display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",height:"100%",gap:12,padding:24}}>
      <div style={{fontSize:44,opacity:0.15}}>⚡</div>
      <div style={{color:T.dim,fontFamily:"'Barlow Condensed',sans-serif",fontSize:11,letterSpacing:3,textTransform:"uppercase",textAlign:"center",lineHeight:1.9}}>Select an athlete<br/>on the map</div>
    </div>
  );

  const dist=haversine(athlete.hometownCoords,athlete.collegeCoords), dc=distColor(dist);
  const hsEvents = Object.keys(athlete.hsTimes);
  const colEvents = Object.keys(athlete.collegeTimes);
  const allEvents = [...new Set([...hsEvents,...colEvents])];

  return(
    <div style={{padding:"16px",overflowY:"auto",height:"100%"}}>
      {/* Header */}
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:12}}>
        <div>
          <div style={{color:T.orange,fontFamily:"'Barlow Condensed',sans-serif",fontSize:21,fontWeight:900,letterSpacing:2}}>{athlete.name}</div>
          <div style={{color:T.offWhite,fontSize:12,marginTop:2}}>{athlete.college}</div>
          <div style={{color:T.muted,fontSize:11}}>{athlete.conference} · Year {athlete.collegeYear}</div>
        </div>
        <button onClick={onClose} style={{background:"none",border:`1px solid ${T.border}`,color:T.muted,borderRadius:6,cursor:"pointer",width:26,height:26,fontSize:13,display:"flex",alignItems:"center",justifyContent:"center"}}>✕</button>
      </div>

      {/* Distance card */}
      <div style={{background:`${T.bgCard}`,border:`1px solid ${dc}55`,borderRadius:9,padding:"10px 12px",marginBottom:12}}>
        <div style={{color:T.muted,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:2,textTransform:"uppercase"}}>Recruitment Distance</div>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginTop:4}}>
          <div>
            <div style={{color:dc,fontSize:24,fontWeight:900,fontFamily:"'Barlow Condensed',sans-serif",lineHeight:1}}>{fmtDist(dist)}</div>
            <div style={{color:T.muted,fontSize:10,marginTop:3}}>{athlete.hometown} → {athlete.college}</div>
          </div>
          <div style={{fontSize:26}}>{dist<100?"🏠":dist<400?"🚗":dist<800?"✈️":"🌎"}</div>
        </div>
        <div style={{marginTop:7,display:"flex",gap:5,alignItems:"center"}}>
          <div style={{width:7,height:7,borderRadius:"50%",background:dc}}/>
          <span style={{color:dc,fontSize:11}}>{distLabel(dist)}</span>
        </div>
      </div>

      {/* Info grid */}
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6,marginBottom:12}}>
        {[{l:"Hometown",v:athlete.hometown},{l:"High School",v:athlete.hsName},{l:"HS Grad",v:athlete.hsYear},{l:"College Year",v:`Year ${athlete.collegeYear}`}].map(({l,v})=>(
          <div key={l} style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:6,padding:"7px 9px"}}>
            <div style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase"}}>{l}</div>
            <div style={{color:T.offWhite,fontSize:11,marginTop:2,fontWeight:500}}>{v}</div>
          </div>
        ))}
      </div>

      {/* Event tags */}
      <div style={{display:"flex",gap:3,flexWrap:"wrap",marginBottom:14}}>
        {athlete.events.map(e=>{
          const cfg=EVENTS_CFG.find(x=>x.id===e);
          const col=cfg?.season==="indoor"?T.blueL:cfg?.season==="outdoor"?T.green:T.orange;
          return <span key={e} style={{background:`${col}22`,color:col,borderRadius:4,padding:"2px 8px",fontSize:11,border:`1px solid ${col}44`,fontFamily:"'Barlow Condensed',sans-serif"}}>{e}</span>;
        })}
      </div>

      {/* ── PERFORMANCES BY YEAR ── */}
      <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:10}}>
        <div style={{flex:1,height:1,background:`${T.orange}28`}}/>
        <span style={{color:T.orange,fontSize:10,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:2,textTransform:"uppercase"}}>Performances by Year</span>
        <div style={{flex:1,height:1,background:`${T.orange}28`}}/>
      </div>

      {/* HS / College toggle */}
      <div style={{display:"flex",background:T.bg,borderRadius:7,border:`1px solid ${T.border}`,overflow:"hidden",marginBottom:12}}>
        {[["hs","🏫 High School"],["college","🎓 College"]].map(([tab,lbl])=>(
          <button key={tab} onClick={()=>setPerfTab(tab)} style={{flex:1,padding:"7px 6px",background:perfTab===tab?T.orange:"transparent",border:"none",color:perfTab===tab?T.white:T.muted,fontSize:11,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",transition:"all 0.15s"}}>{lbl}</button>
        ))}
      </div>

      {perfTab === "hs" ? (
        <div>
          {/* Year header */}
          <div style={{display:"grid",gridTemplateColumns:"80px 1fr 1fr 1fr 1fr",gap:2,marginBottom:4,paddingBottom:4,borderBottom:`1px solid ${T.border}`}}>
            <div style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase"}}>Event</div>
            {["Fr","So","Jr","Sr"].map((lbl,i)=>(
              <div key={lbl} style={{textAlign:"center",color:i===3?T.orange:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",fontWeight:i===3?700:400,letterSpacing:1}}>
                {lbl}<br/>
                <span style={{color:T.dim,fontSize:8,fontWeight:400}}>{athlete.hsYear-3+i}</span>
              </div>
            ))}
          </div>
          {hsEvents.length === 0 ? (
            <div style={{color:T.dim,fontSize:11,textAlign:"center",padding:"16px 0"}}>No timed events recorded</div>
          ) : hsEvents.map(ev => {
            const rows = genHSTimes(athlete, ev);
            const best = Math.min(...rows.map(r=>r.time));
            return (
              <div key={ev} style={{display:"grid",gridTemplateColumns:"80px 1fr 1fr 1fr 1fr",gap:2,padding:"5px 0",borderBottom:`1px solid ${T.border}44`}}>
                <div style={{color:T.offWhite,fontSize:11,fontFamily:"'Barlow Condensed',sans-serif",fontWeight:600,alignSelf:"center"}}>{ev}</div>
                {rows.map(r=>(
                  <div key={r.label} style={{textAlign:"center",padding:"3px 2px",borderRadius:4,background:r.isBest?`${T.orange}22`:"transparent",border:r.isBest?`1px solid ${T.orange}55`:"1px solid transparent"}}>
                    <div style={{color:r.isBest?T.orange:T.offWhite,fontSize:11,fontWeight:r.isBest?800:400,fontFamily:"monospace",lineHeight:1.3}}>{fmtTime(r.time)}</div>
                    {r.isBest&&<div style={{color:T.orange,fontSize:7,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>BEST</div>}
                  </div>
                ))}
              </div>
            );
          })}
          {/* HS summary row */}
          {hsEvents.length > 0 && (
            <div style={{marginTop:10,padding:"8px 10px",background:T.bgCard,borderRadius:7,border:`1px solid ${T.border}`}}>
              <div style={{color:T.dim,fontSize:9,letterSpacing:1,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:4}}>Senior Year Top Marks</div>
              <div style={{display:"flex",gap:10,flexWrap:"wrap"}}>
                {hsEvents.map(ev=>(
                  <div key={ev} style={{display:"flex",gap:4,alignItems:"baseline"}}>
                    <span style={{color:T.muted,fontSize:10,fontFamily:"'Barlow Condensed',sans-serif"}}>{ev}</span>
                    <span style={{color:T.orange,fontSize:12,fontWeight:800,fontFamily:"monospace"}}>{fmtTime(athlete.hsTimes[ev])}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      ) : (
        <div>
          {/* Year header */}
          <div style={{display:"grid",gridTemplateColumns:"80px 1fr 1fr 1fr 1fr",gap:2,marginBottom:4,paddingBottom:4,borderBottom:`1px solid ${T.border}`}}>
            <div style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase"}}>Event</div>
            {[1,2,3,4].map(y=>(
              <div key={y} style={{textAlign:"center",color:y===athlete.collegeYear?T.orange:y>athlete.collegeYear?T.dim:T.offWhite,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",fontWeight:y===athlete.collegeYear?700:400,letterSpacing:1}}>
                Y{y}<br/>
                <span style={{fontSize:8,fontWeight:400,opacity:y>athlete.collegeYear?0.4:1}}>{athlete.hsYear+y}</span>
              </div>
            ))}
          </div>
          {colEvents.length === 0 ? (
            <div style={{color:T.dim,fontSize:11,textAlign:"center",padding:"16px 0"}}>No timed events recorded</div>
          ) : colEvents.map(ev => {
            const rows = genCollegeTimes(athlete, ev);
            return (
              <div key={ev} style={{display:"grid",gridTemplateColumns:"80px 1fr 1fr 1fr 1fr",gap:2,padding:"5px 0",borderBottom:`1px solid ${T.border}44`}}>
                <div style={{color:T.offWhite,fontSize:11,fontFamily:"'Barlow Condensed',sans-serif",fontWeight:600,alignSelf:"center"}}>{ev}</div>
                {rows.map(r=>(
                  <div key={r.label} style={{textAlign:"center",padding:"3px 2px",borderRadius:4,
                    background:r.isCurrent?`${T.orange}22`:r.isFuture?"transparent":"transparent",
                    border:r.isCurrent?`1px solid ${T.orange}55`:"1px solid transparent",
                    opacity:r.isFuture?0.35:1}}>
                    <div style={{color:r.isCurrent?T.orange:r.isFuture?T.dim:T.offWhite,fontSize:11,fontWeight:r.isCurrent?800:400,fontFamily:"monospace",lineHeight:1.3}}>{fmtTime(r.time)}</div>
                    {r.isCurrent&&<div style={{color:T.orange,fontSize:7,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>NOW</div>}
                    {r.isFuture&&<div style={{color:T.dim,fontSize:7,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>PROJ</div>}
                  </div>
                ))}
              </div>
            );
          })}
          {/* HS→College comparison row */}
          {colEvents.length > 0 && (
            <div style={{marginTop:10,padding:"8px 10px",background:T.bgCard,borderRadius:7,border:`1px solid ${T.border}`}}>
              <div style={{color:T.dim,fontSize:9,letterSpacing:1,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>HS Best → Current PR</div>
              {colEvents.filter(ev=>athlete.hsTimes[ev]).map(ev=>{
                const diff = athlete.hsTimes[ev] - athlete.collegeTimes[ev];
                return(
                  <div key={ev} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"3px 0"}}>
                    <span style={{color:T.muted,fontSize:11,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>{ev}</span>
                    <div style={{display:"flex",gap:8,alignItems:"center"}}>
                      <span style={{color:T.dim,fontSize:11,fontFamily:"monospace"}}>{fmtTime(athlete.hsTimes[ev])}</span>
                      <span style={{color:T.dim,fontSize:10}}>→</span>
                      <span style={{color:T.orange,fontSize:12,fontWeight:800,fontFamily:"monospace"}}>{fmtTime(athlete.collegeTimes[ev])}</span>
                      {diff>0&&<span style={{color:T.green,fontSize:10,fontFamily:"monospace",background:`${T.green}18`,borderRadius:3,padding:"1px 4px"}}>▼{diff.toFixed(2)}</span>}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── APP ───────────────────────────────────────────────────────────────────────
const BLANK_FILTERS = {events:[],conference:"",college:"",hsYear:"",collegeYear:"",season:"all"};

export default function App(){
  const [selectedAthlete,setSelectedAthlete]=useState(null);
  const [mapMode,setMapMode]=useState("flows");
  const [rightTab,setRightTab]=useState("athlete");
  const [focusedCollege,setFocusedCollege]=useState("");
  const [focusedHometown,setFocusedHometown]=useState("");
  const [selectedStates,setSelectedStates]=useState([]);
  const [filters,setFilters]=useState({...BLANK_FILTERS});
  const [search,setSearch]=useState("");

  const filtered=useMemo(()=>applyFilters(ATHLETES,filters,search),[filters,search]);
  const overallAvg=useMemo(()=>filtered.length?Math.round(filtered.reduce((s,a)=>s+haversine(a.hometownCoords,a.collegeCoords),0)/filtered.length):0,[filtered]);
  const hasFilters=filters.events.length>0||filters.conference||filters.college||filters.hsYear||filters.collegeYear||search||filters.season!=="all"||selectedStates.length>0;

  const handleAthleteClick=a=>{setSelectedAthlete(s=>s?.id===a.id?null:a);setRightTab("athlete");};
  const handleFocusCollege=c=>{setFocusedCollege(c);if(c)setFocusedHometown("");};
  const handleFocusHometown=h=>{setFocusedHometown(h);if(h)setFocusedCollege("");};
  const switchMapMode=m=>{setMapMode(m);if(m==="heatmap")setRightTab("heatmap");};
  const switchRightTab=t=>{setRightTab(t);if(t==="heatmap")setMapMode("heatmap");else if(mapMode==="heatmap")setMapMode("flows");};

  const highlightCollege=rightTab==="college"?focusedCollege:"";
  const highlightHometown=rightTab==="hometown"?focusedHometown:"";

  const confColleges = getConfColleges(filters.conference);

  return(
    <>
      <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:ital,wght@0,400;0,600;0,700;0,800;0,900;1,700&family=Barlow:wght@400;500;600&display=swap" rel="stylesheet"/>
      <style>{`*{box-sizing:border-box;margin:0;padding:0;}::-webkit-scrollbar{width:5px;}::-webkit-scrollbar-track{background:#F7F7F8;}::-webkit-scrollbar-thumb{background:#D4D6D9;border-radius:3px;}::-webkit-scrollbar-thumb:hover{background:#F76900;}button:hover{filter:brightness(0.95);}select option{background:#FFFFFF;color:#404040;}input::placeholder{color:#ADB3B8;}`}</style>
      <div style={{height:"100vh",display:"flex",flexDirection:"column",background:T.bg,color:T.offWhite,fontFamily:"'Barlow',sans-serif",overflow:"hidden"}}>

        {/* HEADER */}
        <div style={{padding:"9px 18px",background:T.bgPanel,borderBottom:`2px solid ${T.orange}`,display:"flex",alignItems:"center",gap:14,flexShrink:0,boxShadow:`0 2px 20px rgba(247,105,0,0.2)`}}>
          <div style={{display:"flex",alignItems:"center",gap:10}}>
            <div style={{width:34,height:34,borderRadius:7,background:`linear-gradient(135deg,${T.orange},${T.orangeD})`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:19,boxShadow:`0 0 20px ${T.orangeGlow}`}}>⚡</div>
            <div>
              <div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:20,fontWeight:900,letterSpacing:4,color:T.blueP,textTransform:"uppercase"}}>Run Orange</div>
              <div style={{fontSize:9,color:T.muted,letterSpacing:3,textTransform:"uppercase",marginTop:-2}}>Recruiting Intelligence</div>
            </div>
          </div>

          <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search athletes, colleges, cities…"
            style={{flex:1,maxWidth:300,background:"#F7F7F8",border:`1px solid ${T.border}`,borderRadius:7,padding:"6px 12px",color:T.offWhite,fontSize:12,fontFamily:"'Barlow',sans-serif",outline:"none"}}/>

          <div style={{display:"flex",background:T.bgCard,borderRadius:7,border:`1px solid ${T.border}`,overflow:"hidden"}}>
            {[["flows","⟿ Flows"],["hometown","📍 Home"],["college","🎓 College"],["heatmap","🌡 Heat"]].map(([m,l])=>(
              <button key={m} onClick={()=>switchMapMode(m)} style={{background:mapMode===m?T.orange:"transparent",border:"none",color:mapMode===m?T.white:T.muted,padding:"6px 12px",fontSize:11,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",transition:"all 0.15s",fontWeight:mapMode===m?700:400}}>{l}</button>
            ))}
          </div>

          <div style={{background:T.orangeGlow,border:`1px solid ${T.orange}44`,borderRadius:7,padding:"5px 12px",textAlign:"center"}}>
            <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase"}}>Avg Distance</div>
            <div style={{color:T.orange,fontSize:16,fontWeight:900,fontFamily:"'Barlow Condensed',sans-serif"}}>{overallAvg.toLocaleString()} mi</div>
          </div>
          <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:7,padding:"5px 12px",textAlign:"center"}}>
            <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase"}}>Athletes</div>
            <div style={{color:T.offWhite,fontSize:16,fontWeight:900,fontFamily:"'Barlow Condensed',sans-serif"}}>{filtered.length}<span style={{color:T.dim,fontSize:11}}>/{ATHLETES.length}</span></div>
          </div>
        </div>

        <div style={{flex:1,display:"flex",overflow:"hidden"}}>

          {/* LEFT FILTER PANEL */}
          <div style={{width:208,background:T.bgPanel,borderRight:`1px solid ${T.border}`,padding:"12px 11px",overflowY:"auto",flexShrink:0}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}>
              <span style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:11,letterSpacing:2,color:T.orange,textTransform:"uppercase"}}>Main Filters</span>
              {hasFilters&&<button onClick={()=>{setFilters({...BLANK_FILTERS});setSearch("");setSelectedStates([]);}} style={{background:"none",border:"none",color:T.red,fontSize:10,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>CLEAR</button>}
            </div>

            <FilterControls filters={filters} setFilters={setFilters} showSeason={true} selectedStates={selectedStates} onStatesChange={setSelectedStates}/>

            <div style={{borderTop:`1px solid ${T.border}`,paddingTop:10,marginTop:4}}>
              <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Athletes ({filtered.length})</div>
              {filtered.slice(0,80).map(a=>{
                const d=Math.round(haversine(a.hometownCoords,a.collegeCoords));
                const isSel=selectedAthlete?.id===a.id;
                return(
                  <button key={a.id} onClick={()=>handleAthleteClick(a)} style={{display:"block",width:"100%",textAlign:"left",background:isSel?T.orangeGlow:"rgba(255,255,255,0.02)",border:`1px solid ${isSel?T.orange:T.border}`,borderRadius:5,padding:"5px 7px",marginBottom:2,cursor:"pointer",transition:"all 0.12s"}}>
                    <div style={{color:isSel?T.orange:T.offWhite,fontSize:12,fontWeight:700,fontFamily:"'Barlow Condensed',sans-serif"}}>{a.name}</div>
                    <div style={{display:"flex",justifyContent:"space-between",marginTop:1}}>
                      <span style={{color:T.dim,fontSize:10,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",maxWidth:110}}>{a.college}</span>
                      <span style={{color:distColor(d),fontSize:10,fontFamily:"monospace"}}>{fmtDist(d)}</span>
                    </div>
                  </button>
                );
              })}
              {filtered.length>80&&<div style={{color:T.dim,fontSize:10,textAlign:"center",marginTop:6}}>+{filtered.length-80} more (use filters)</div>}
            </div>
          </div>

          {/* MAP */}
          <div style={{flex:1,position:"relative",overflow:"hidden"}}>
            <USMap athletes={filtered} onAthleteClick={handleAthleteClick} selectedAthlete={selectedAthlete} highlightCollege={highlightCollege} highlightHometown={highlightHometown} mapMode={mapMode} selectedStates={selectedStates}/>

            {/* Selected states indicator */}
            {selectedStates.length>0 && mapMode!=="heatmap" && (
              <div style={{position:"absolute",top:14,left:"50%",transform:"translateX(-50%)",background:"#FFFFFF",border:`2px solid ${T.orange}`,borderRadius:20,padding:"5px 16px",boxShadow:`0 2px 16px rgba(247,105,0,0.2)`,display:"flex",alignItems:"center",gap:10,zIndex:10,whiteSpace:"nowrap"}}>
                <div style={{width:7,height:7,borderRadius:"50%",background:T.orange}}/>
                <span style={{color:T.orange,fontFamily:"'Barlow Condensed',sans-serif",fontSize:12,fontWeight:800,letterSpacing:1}}>
                  {selectedStates.length===1?STATE_NAMES[selectedStates[0]]:selectedStates.length+" States"} · {filtered.filter(a=>selectedStates.includes(getState(a.hometown))).length} athletes
                </span>
                <button onClick={()=>setSelectedStates([])} style={{background:"none",border:`1px solid ${T.border}`,color:T.muted,borderRadius:10,cursor:"pointer",fontSize:10,padding:"1px 7px",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>✕ Clear</button>
              </div>
            )}

            {/* Legend */}
            <div style={{position:"absolute",bottom:14,left:14,background:"rgba(255,255,255,0.96)",border:`1px solid ${T.border}`,borderRadius:9,padding:"9px 13px",boxShadow:`0 2px 16px rgba(0,0,0,0.10)`}}>
              {mapMode==="heatmap"?(
                <div>
                  <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Hometown Density</div>
                  <div style={{display:"flex",gap:3,alignItems:"center",marginBottom:4}}>
                    {["#000E54","#D74100","#F76900","#FF8E00","#FFFFFF"].map((c,i)=><div key={i} style={{width:22,height:7,background:c,borderRadius:1}}/>)}
                  </div>
                  <div style={{display:"flex",justifyContent:"space-between"}}><span style={{color:T.dim,fontSize:9}}>Low</span><span style={{color:T.dim,fontSize:9}}>High</span></div>
                  <div style={{color:T.muted,fontSize:9,marginTop:4}}>{filtered.length} athletes shown</div>
                </div>
              ):mapMode==="flows"?(
                <div>
                  <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Arc = Distance</div>
                  <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
                    {[[T.green,"Local"],[T.yellow,"Regional"],[T.orange,"Long Haul"],[T.red,"Cross-Country"]].map(([c,l])=>(
                      <div key={l} style={{display:"flex",gap:5,alignItems:"center"}}><div style={{width:18,height:2,background:c,borderRadius:1}}/><span style={{color:T.muted,fontSize:10}}>{l}</span></div>
                    ))}
                  </div>
                  <div style={{display:"flex",gap:12,marginTop:5}}>
                    <div style={{display:"flex",gap:5,alignItems:"center"}}><div style={{width:7,height:7,borderRadius:"50%",background:T.muted}}/><span style={{color:T.muted,fontSize:10}}>● Hometown</span></div>
                    <div style={{display:"flex",gap:5,alignItems:"center"}}><div style={{width:7,height:7,background:T.muted,borderRadius:1}}/><span style={{color:T.muted,fontSize:10}}>▪ College</span></div>
                  </div>
                  <div style={{marginTop:6,color:T.dim,fontSize:9,fontStyle:"italic"}}>Filter by state using the left panel</div>
                </div>
              ):(
                <div style={{display:"flex",gap:12,alignItems:"center"}}>
                  <div style={{display:"flex",gap:5,alignItems:"center"}}><div style={{width:8,height:8,borderRadius:"50%",background:T.orangeL}}/><span style={{color:T.muted,fontSize:10}}>Athlete</span></div>
                  <div style={{display:"flex",gap:5,alignItems:"center"}}><div style={{width:10,height:10,borderRadius:"50%",background:T.orange,border:`2px solid ${T.white}`}}/><span style={{color:T.muted,fontSize:10}}>Selected</span></div>
                  <span style={{color:T.dim,fontSize:10}}>{mapMode==="college"?"College":"Hometown"} view</span>
                </div>
              )}
            </div>
          </div>

          {/* RIGHT TABBED PANEL */}
          <div style={{width:275,background:T.bgPanel,borderLeft:`1px solid ${T.border}`,display:"flex",flexDirection:"column",flexShrink:0}}>
            <div style={{display:"flex",borderBottom:`1px solid ${T.border}`,flexShrink:0}}>
              {[["athlete","Athlete"],["college","Pull"],["hometown","Origin"],["heatmap","🌡 Heat"]].map(([tab,label])=>(
                <button key={tab} onClick={()=>switchRightTab(tab)} style={{flex:1,padding:"8px 3px",background:rightTab===tab?T.orangeGlow:"transparent",border:"none",borderBottom:rightTab===tab?`2px solid ${T.orange}`:"2px solid transparent",color:rightTab===tab?T.orange:T.muted,fontSize:10,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",transition:"all 0.12s"}}>{label}</button>
              ))}
            </div>
            <div style={{flex:1,overflowY:"auto"}}>
              {rightTab==="athlete"  && <AthleteDetail athlete={selectedAthlete} onClose={()=>setSelectedAthlete(null)}/>}
              {rightTab==="college"  && <CollegePullPanel athletes={filtered} focusedCollege={focusedCollege} onFocusCollege={handleFocusCollege}/>}
              {rightTab==="hometown" && <HometownPanel athletes={filtered} focusedHometown={focusedHometown} onFocusHometown={handleFocusHometown}/>}
              {rightTab==="heatmap"  && <HeatmapPanel athletes={filtered}/>}
            </div>
          </div>

        </div>
      </div>
    </>
  );
}

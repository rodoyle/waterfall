// Palantir prototype: the adapter accepts the versioned contract in docs/palantir-response.schema.json.
// Visions are composited INTO the mist shader as textures — media never renders as overlay HTML
// when WebGL is available. Caption cards below the orb carry only text.
/** @typedef {'image'|'video'|'video-frame'|'raster-text'} VisionKind */
/** @typedef {{kind: VisionKind, src?: string, caption: string, effect?: string}} VisionItem */
/** @typedef {{version: 'palantir.v1', prompt: string, items: VisionItem[]}} PalantirResponse */
const canvas = document.querySelector("#orb"),
  gl = canvas.getContext("webgl");
const status = document.querySelector("#status"),
  visions = document.querySelector("#visions");
let frame = 0,
  program,
  media = []; // media: [{tex, kind, aspect, video?, ready, revealedAt}]
const MAX_VISIONS = 3;

const FRAG = `precision highp float;
uniform float t;uniform vec2 r;
uniform sampler2D uTex0,uTex1,uTex2;
uniform float uAspect0,uAspect1,uAspect2;
uniform float uReveal0,uReveal1,uReveal2;
float n(vec2 p){return fract(sin(dot(p,vec2(12.9898,78.233)))*43758.5453);}
vec3 sampleVision(sampler2D tex,float aspect,float reveal,vec2 u,float d,float ball,float idx){
  if(reveal<=0.)return vec3(0.);
  // Spread the visions out smoothly within the orb
  float angle = t * 0.15 + idx * 2.0944; // 120 degrees apart
  vec2 orbit = vec2(cos(angle) * 0.16, sin(angle) * 0.10);
  vec2 c = u - orbit;
  // Subtle organic fluid drift without destroying image coherence
  c += 0.015 * vec2(sin(c.y * 6.0 + t * 0.8), cos(c.x * 5.0 - t * 0.7)) * ball;
  // Larger, clearer aperture for each vision
  float radius = 0.28;
  float lens = smoothstep(radius, radius * 0.55, length(c));
  if(lens<=0.)return vec3(0.);
  // Cover-fit coordinate mapping
  vec2 cuv = c / radius;
  vec2 tuv = (aspect > 1.0) ? vec2(0.5 + cuv.x * 0.5 / aspect, 0.5 + cuv.y * 0.5) : vec2(0.5 + cuv.x * 0.5, 0.5 + cuv.y * 0.5 * aspect);
  tuv = clamp(tuv, 0.0, 1.0);
  vec3 col = texture2D(tex, tuv).rgb;
  // Boost contrast and preserve natural highlights/colors while still feeling ethereal
  col = mix(vec3(0.08, 0.04, 0.16), col, 0.92);
  float fade = smoothstep(0.0, 1.0, reveal);
  // Gentle luminescent edge glow and soft mist integration
  vec3 rim = vec3(0.12, 0.08, 0.22) * pow(1.0 - length(cuv), 2.0);
  return (col * fade + rim) * lens * ball;
}
void main(){
  vec2 u=(gl_FragCoord.xy-.5*r)/r.y;
  float d=length(u-vec2(0.,.02));
  float ball=smoothstep(.48,.38,d);
  float mist=sin(u.x*8.+t*.3+sin(u.y*7.-t)*2.)*.04+sin(u.y*15.-t)*.025;
  float glow=exp(-d*5.)*(.55+mist);
  vec3 c=mix(vec3(.02,.01,.06),vec3(.16,.08,.3),ball);
  c+=sampleVision(uTex0,uAspect0,uReveal0,u,d,ball,0.0);
  c+=sampleVision(uTex1,uAspect1,uReveal1,u,d,ball,1.0);
  c+=sampleVision(uTex2,uAspect2,uReveal2,u,d,ball,2.0);
  c+=vec3(.18,.1,.38)*glow;
  c+=vec3(.03,.22,.28)*max(0.,mist*8.)*ball;
  c+=vec3(.06,.04,.12)*n(u*300.+t)*ball; // living grain inside the glass
  gl_FragColor=vec4(c,1.);
}`;

function makeTexture() {
  const tex = gl.createTexture();
  gl.bindTexture(gl.TEXTURE_2D, tex);
  gl.texImage2D(
    gl.TEXTURE_2D,
    0,
    gl.RGBA,
    1,
    1,
    0,
    gl.RGBA,
    gl.UNSIGNED_BYTE,
    new Uint8Array([10, 6, 24, 255]),
  );
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
  return tex;
}
function loadVision(item, index) {
  const entry = {
    tex: makeTexture(),
    kind: item.kind,
    aspect: 1,
    video: null,
    ready: false,
    revealedAt: 0,
  };
  media[index] = entry;
  const onReady = (el, w, h) => {
    entry.ready = true;
    entry.revealedAt = frame;
    entry.aspect = w && h ? w / h : 1;
    gl.bindTexture(gl.TEXTURE_2D, entry.tex);
    try {
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, el);
    } catch {
      entry.ready = false;
    }
  };
  if (item.kind === "video") {
    const v = document.createElement("video");
    v.crossOrigin = "anonymous";
    v.src = item.src || "";
    v.muted = true;
    v.loop = true;
    v.playsInline = true;
    v.addEventListener(
      "loadeddata",
      () => {
        v.play().catch(() => {});
        onReady(v, v.videoWidth, v.videoHeight);
      },
      { once: true },
    );
    v.onerror = () => {
      entry.ready = false;
    };
    entry.video = v;
    v.load();
  } else {
    const img = new Image();
    img.crossOrigin = "anonymous";
    img.src = item.src || "";
    img.onload = () => onReady(img, img.naturalWidth, img.naturalHeight);
    img.onerror = () => {
      entry.ready = false;
    };
  }
}
function releaseMedia() {
  for (const m of media)
    if (m && m.video) {
      m.video.pause();
      m.video.src = "";
    }
  media = [];
}

function fallback() {
  canvas.style.background =
    "radial-gradient(circle at 50% 45%,#43316b 0,#120d26 28%,#05040b 65%)";
  status.textContent = "WebGL unavailable — using the quiet-light fallback.";
}
if (gl) {
  const compile = (type, src) => {
    const s = gl.createShader(type);
    gl.shaderSource(s, src);
    gl.compileShader(s);
    return s;
  };
  const vs = compile(
    gl.VERTEX_SHADER,
    "attribute vec2 p;void main(){gl_Position=vec4(p,0.,1.);}",
  );
  const fs = compile(gl.FRAGMENT_SHADER, FRAG);
  program = gl.createProgram();
  gl.attachShader(program, vs);
  gl.attachShader(program, fs);
  gl.linkProgram(program);
  const b = gl.createBuffer();
  gl.bindBuffer(gl.ARRAY_BUFFER, b);
  gl.bufferData(
    gl.ARRAY_BUFFER,
    new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]),
    gl.STATIC_DRAW,
  );
  const p = gl.getAttribLocation(program, "p");
  gl.enableVertexAttribArray(p);
  gl.vertexAttribPointer(p, 2, gl.FLOAT, false, 0, 0);
  const U = (name) => gl.getUniformLocation(program, name);
  const u = {
    t: U("t"),
    r: U("r"),
    reveal: [U("uReveal0"), U("uReveal1"), U("uReveal2")],
    aspect: [U("uAspect0"), U("uAspect1"), U("uAspect2")],
    tex: [U("uTex0"), U("uTex1"), U("uTex2")],
  };
  const tick = () => {
    canvas.width = innerWidth * devicePixelRatio;
    canvas.height = innerHeight * devicePixelRatio;
    gl.viewport(0, 0, canvas.width, canvas.height);
    gl.useProgram(program);
    gl.uniform1f(u.t, frame++ * 0.016);
    gl.uniform2f(u.r, canvas.width, canvas.height);
    for (let i = 0; i < MAX_VISIONS; i++) {
      gl.activeTexture(gl.TEXTURE0 + i);
      const m = media[i];
      if (m && m.ready && m.video) {
        // live video frames are re-uploaded every frame
        gl.bindTexture(gl.TEXTURE_2D, m.tex);
        try {
          gl.texImage2D(
            gl.TEXTURE_2D,
            0,
            gl.RGBA,
            gl.RGBA,
            gl.UNSIGNED_BYTE,
            m.video,
          );
        } catch {
          m.ready = false;
        }
      } else if (m) {
        gl.bindTexture(gl.TEXTURE_2D, m.tex);
      } else {
        gl.bindTexture(gl.TEXTURE_2D, makeTexture());
      }
      gl.uniform1i(u.tex[i], i);
      gl.uniform1f(u.aspect[i], m ? m.aspect : 1);
      gl.uniform1f(
        u.reveal[i],
        m && m.ready ? Math.min((frame - m.revealedAt) * 0.016 * 2.2, 1.6) : 0,
      );
    }
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    requestAnimationFrame(tick);
  };
  tick();
} else fallback();

const mock = (prompt) => ({
  version: "palantir.v1",
  prompt,
  items: [
    {
      kind: "image",
      src: "https://images.unsplash.com/photo-1511497584788-876760111969?auto=format&fit=crop&w=900&q=80",
      caption: "The garden, after dusk",
      effect: "nebula",
    },
    {
      kind: "video",
      src: "https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4",
      caption: "A motion trace from the garden",
      effect: "bloom",
    },
    {
      kind: "raster-text",
      src: "https://images.unsplash.com/photo-1519681393784-d120267933ba?auto=format&fit=crop&w=900&q=80",
      caption: "“Something is moving beyond the hedge.”",
      effect: "chromatic",
    },
  ],
});
function parseResponse(payload) {
  if (
    !payload ||
    payload.version !== "palantir.v1" ||
    typeof payload.prompt !== "string" ||
    !Array.isArray(payload.items)
  )
    throw new Error("Invalid palantir response envelope");
  const items = payload.items.map((item, index) => {
    if (
      !item ||
      !["image", "video", "video-frame", "raster-text"].includes(item.kind) ||
      typeof item.caption !== "string" ||
      (item.src !== undefined && typeof item.src !== "string")
    )
      throw new Error(`Invalid vision item ${index}`);
    return item;
  });
  return { ...payload, items };
}
async function fetchResponse(prompt) {
  const mode = new URLSearchParams(location.search).get("fixture");
  if (mode === "empty")
    return parseResponse({ version: "palantir.v1", prompt, items: [] });
  if (mode === "malformed")
    return parseResponse({
      version: "palantir.v1",
      prompt,
      items: [{ kind: "unknown", caption: "" }],
    });
  return parseResponse(mock(prompt));
}

function render(response) {
  if (!response) {
    status.textContent = "The oracle returned an unreadable vision.";
    return;
  }
  if (!response.items.length) {
    status.textContent =
      "The mist cleared without a vision. Try another question.";
    return;
  }
  releaseMedia();
  response.items
    .slice(0, gl ? MAX_VISIONS : Infinity)
    .forEach((item, index) => {
      if (gl) loadVision(item, index);
    });
  visions.replaceChildren(
    ...response.items.map((item) => {
      const card = document.createElement("article");
      card.className = "vision";
      // Media renders inside the orb shader, not here — this card is the echo/whisper.
      const p = document.createElement("p");
      p.textContent = item.caption || "";
      const small = document.createElement("small");
      small.textContent = `${item.kind} · ${item.effect || "mist"} · within the orb`;
      card.append(p, small);
      return card;
    }),
  );
  if (!gl) {
    // no-shader fallback: show media as DOM so the page still works
    [...visions.children].forEach((card, i) => {
      const item = response.items[i];
      if (!item || !item.src) return;
      if (item.kind === "video") {
        const v = document.createElement("video");
        v.src = item.src;
        v.controls = true;
        v.autoplay = true;
        v.muted = true;
        v.onerror = () => {
          status.textContent =
            "A video vision could not be loaded; the other visions remain available.";
        };
        card.prepend(v);
      } else {
        const img = document.createElement("img");
        img.src = item.src;
        img.alt = item.caption || "Oracle vision";
        img.onerror = () => {
          img.replaceWith(
            Object.assign(document.createElement("div"), {
              textContent: "Vision unavailable",
            }),
          );
        };
        card.prepend(img);
      }
    });
  }
}
const form = document.querySelector("#oracle");
form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const prompt = document.querySelector("#prompt").value.trim();
  if (!prompt) {
    status.textContent = "Ask the orb a question first.";
    return;
  }
  status.textContent = "Mist gathers… resolving your vision.";
  visions.replaceChildren();
  releaseMedia();
  await new Promise((r) => setTimeout(r, 1100));
  try {
    render(await fetchResponse(prompt));
    if (visions.children.length)
      status.textContent = "The vision has resolved. Look into the orb.";
  } catch (error) {
    status.textContent = `The oracle response was invalid: ${error.message}`;
  }
});
document.querySelector("#reset").addEventListener("click", () => {
  visions.replaceChildren();
  releaseMedia();
  status.textContent = "The orb is listening.";
  document.querySelector("#prompt").focus();
});

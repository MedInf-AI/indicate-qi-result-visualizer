(() => {
  // tooltip + highlight of the same period in every panel of a figure
  const tip = document.getElementById('tip');
  let lit = [];
  document.addEventListener('mouseover', e => {
    const h = e.target.closest && e.target.closest('rect.hit');
    lit.forEach(r => r.classList.remove('on'));
    lit = [];
    const fr = !h && e.target.closest && e.target.closest('svg.flame g[data-tip]');
    if (fr) { tip.textContent = fr.dataset.tip; tip.hidden = false; return; }
    if (!h) { tip.hidden = true; return; }
    const fig = h.closest('.fig') || document;
    lit = [...fig.querySelectorAll(`rect.hit[data-b="${h.dataset.b}"]`)];
    lit.forEach(r => r.classList.add('on'));
    tip.textContent = h.dataset.tip;
    tip.hidden = false;
  });
  document.addEventListener('mousemove', e => {
    if (tip.hidden) return;
    const r = tip.getBoundingClientRect();
    let x = e.clientX + 14, y = e.clientY + 14;
    if (x + r.width > innerWidth - 8) x = e.clientX - r.width - 14;
    if (y + r.height > innerHeight - 8) y = e.clientY - r.height - 14;
    tip.style.left = x + 'px'; tip.style.top = y + 'px';
  });

  // flamegraphs: fetched when their section is first opened; zooming stretches the
  // x axis only, so that labels keep their size
  function flame(wrap) {
    const svg = wrap.querySelector('svg.flame');
    const W = +svg.getAttribute('width');
    const els = [...svg.querySelectorAll('rect, text')].map(el =>
      [el, +el.getAttribute('x'), el.tagName === 'rect' ? +el.getAttribute('width') : null]);
    let scale = 1;
    const MAX = 2e6 / W;  // browsers cannot lay out arbitrarily wide boxes
    function zoom(s, x0) {  // x0: unscaled x to bring to the left edge (default: keep centre)
      const centre = (wrap.scrollLeft + wrap.clientWidth / 2) / scale;
      scale = Math.min(Math.max(s, wrap.clientWidth / W), MAX);
      for (const [el, x, w] of els) {
        el.setAttribute('x', x * scale);
        if (w !== null) el.setAttribute('width', w * scale);
      }
      svg.setAttribute('width', W * scale);
      wrap.scrollLeft = x0 !== undefined ? x0 * scale : centre * scale - wrap.clientWidth / 2;
    }
    wrap.parentElement.querySelector('.flamebar').addEventListener('click', e => {
      const z = e.target.dataset && e.target.dataset.zoom;
      if (!z) return;
      zoom(z === 'fit' ? 0 : z === 'in' ? scale * 2 : z === 'out' ? scale / 2 : +z);
    });
    svg.addEventListener('click', e => {
      const g = e.target.closest('g[data-tip]');
      const r = g && g.querySelector(':scope > rect');
      if (!r) return;
      const i = els.findIndex(([el]) => el === r);
      zoom(wrap.clientWidth / Math.max(els[i][2], 1e-9), els[i][1]);
    });
  }
  document.addEventListener('toggle', async e => {
    const d = e.target;
    if (!d.matches || !d.matches('details.profile') || !d.open) return;
    const wrap = d.querySelector('.flamewrap');
    if (wrap.dataset.loaded) return;
    wrap.dataset.loaded = '1';
    try {
      const r = await fetch(wrap.dataset.src);
      if (!r.ok) throw new Error(await r.text());
      wrap.innerHTML = await r.text();
      flame(wrap);
    } catch (err) {
      wrap.textContent = 'Cannot load the flamegraph: ' + err.message;
      delete wrap.dataset.loaded;
    }
  }, true);

  // result bundles: the pickers switch on change; uploads go up as the raw archive
  ['bundle-date', 'bundle-vs'].forEach(id => {
    const pick = document.getElementById(id);
    if (pick) pick.addEventListener('change', () => pick.form.submit());
  });
  document.querySelectorAll('form.upload').forEach(form => {
    const input = form.querySelector('input[type=file]');
    const button = form.querySelector('button');
    const status = form.querySelector('.meta');
    const say = (text, err) => { status.textContent = text; status.classList.toggle('err', !!err); };
    function send(file, replace) {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/upload?name=' + encodeURIComponent(file.name) + (replace ? '&replace=1' : ''));
      xhr.upload.onprogress = e => {
        if (e.lengthComputable)
          say(e.loaded < e.total ? `Uploading … ${Math.floor(100 * e.loaded / e.total)} %`
                                 : 'Unpacking …');
      };
      xhr.onload = () => {
        let reply = {};
        try { reply = JSON.parse(xhr.responseText); } catch (err) { /* not JSON */ }
        if (xhr.status === 200) {
          say('Done' + (reply.skipped && reply.skipped.length
                        ? ` (skipped: ${reply.skipped.join(', ')})` : '') + '.');
          location.href = reply.url;
        } else if (xhr.status === 409 && confirm(reply.error + '. Replace it?')) {
          send(file, true);
        } else {
          button.disabled = false;
          say('Upload failed: ' + (reply.error || xhr.statusText || 'HTTP ' + xhr.status), true);
        }
      };
      xhr.onerror = () => { button.disabled = false; say('Upload failed: network error', true); };
      button.disabled = true;
      say('Uploading …');
      xhr.send(file);
    }
    form.addEventListener('submit', e => {
      e.preventDefault();
      const file = input.files[0];
      if (!file) return;
      const m = file.name.match(/^results-bundle-(\d{4}-\d\d-\d\d)\.(?:tar\.gz|tgz|tar)$/);
      if (!m) { say('The file must be named results-bundle-YYYY-MM-DD.tar.gz', true); return; }
      const exists = JSON.parse(form.dataset.dates || '[]').includes(m[1]);
      if (exists && !confirm(`There is a bundle for ${m[1]} already. Replace it?`)) return;
      send(file, exists);
    });
  });

  // a link to a section (or into it) opens it if it is collapsed
  function openTarget() {
    const t = location.hash && document.getElementById(decodeURIComponent(location.hash.slice(1)));
    const d = t && (t.querySelector(':scope > details.changes') || t.closest('details'));
    if (d && !d.open) { d.open = true; t.scrollIntoView(); }
  }
  addEventListener('hashchange', openTarget);
  openTarget();

  // sidebar table of contents follows the scroll position
  const links = [...document.querySelectorAll('nav ul.toc a[href^="#"]')];
  const targets = links.map(a => document.getElementById(decodeURIComponent(a.hash.slice(1))));
  let pending = false;
  function spy() {
    pending = false;
    let active = 0;
    targets.forEach((t, i) => { if (t && t.getBoundingClientRect().top <= 120) active = i; });
    if (innerHeight + scrollY >= document.documentElement.scrollHeight - 4)
      active = targets.length - 1;
    links.forEach((a, i) => a.classList.toggle('active', i === active));
    const a = links[active];
    if (a) {
      const nav = a.closest('nav'), ar = a.getBoundingClientRect(), nr = nav.getBoundingClientRect();
      if (ar.top < nr.top || ar.bottom > nr.bottom) a.scrollIntoView({block: 'nearest'});
    }
  }
  if (links.length) {
    addEventListener('scroll', () => { if (!pending) { pending = true; requestAnimationFrame(spy); } },
                     {passive: true});
    spy();
  }
})();

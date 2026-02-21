(function () {
  // Load region flavour script lazily (available well before any user search)
  (function () {
    var s = document.createElement("script");
    s.src = "./js/region-flavour.js";
    document.head.appendChild(s);
  })();

  const stateCode = (window.STATE_CODE || "nsw").toLowerCase();
  const stateName = window.STATE_NAME || stateCode.toUpperCase();

  const state = {
    schools: [],
    postcodeCentroids: {},
    suburbCentroids: [],
    lastRows: [],
    lastSearch: null, // { center, radiusKm, sectorLabel }
  };

  let currentRows = []; // displayed rows (may be filtered by emailsOnly)
  let flavourEl = null;
  let emailsOnlyBtn = null;
  let emailsOnlyActive = false;
  let downloadBtn = null;

  const locationEl = document.getElementById("location");
  const sectorEl = document.getElementById("sector");
  const radiusEl = document.getElementById("radius");
  const searchBtn = document.getElementById("searchBtn");
  const copyBtn = document.getElementById("copyBtn");
  const metaEl = document.getElementById("meta");
  const copyMetaEl = document.getElementById("copyMeta");
  const errEl = document.getElementById("error");
  const tableEl = document.getElementById("results");
  const tbodyEl = tableEl.querySelector("tbody");
  const pageTitle = document.getElementById("pageTitle");
  const pageSub = document.getElementById("pageSub");

  pageTitle.textContent = `${stateName} School Contact Radius Search`;
  pageSub.textContent = `Enter a ${stateName} postcode or suburb, choose radius, and filter schools by distance.`;

  function esc(text) {
    return String(text || "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
  }

  function haversineKm(lat1, lon1, lat2, lon2) {
    const R = 6371;
    const p = Math.PI / 180;
    const dLat = (lat2 - lat1) * p;
    const dLon = (lon2 - lon1) * p;
    const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * p) * Math.cos(lat2 * p) * Math.sin(dLon / 2) ** 2;
    return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  function resolveCenter(rawQuery) {
    const query = rawQuery.trim();
    if (!query) throw new Error("Enter a postcode or suburb.");

    if (/^\d{4}$/.test(query)) {
      const c = state.postcodeCentroids[query];
      if (!c) throw new Error(`No ${stateName} coordinate found for postcode ${query}.`);
      return { lat: c.lat, lon: c.lon, label: `Postcode ${query}` };
    }

    const lower = query.toLowerCase();
    const exact = state.suburbCentroids.find((s) => String(s.suburb).toLowerCase() === lower);
    if (exact) return { lat: exact.lat, lon: exact.lon, label: `Suburb ${exact.suburb}` };

    const partial = state.suburbCentroids.find((s) => String(s.suburb).toLowerCase().includes(lower));
    if (partial) return { lat: partial.lat, lon: partial.lon, label: `Suburb ${partial.suburb}` };

    throw new Error(`Could not resolve location "${query}" to ${stateName} suburb/postcode.`);
  }

  function renderRows(rows) {
    tbodyEl.innerHTML = rows.map((r) => `
      <tr>
        <td>${esc(r.school_name)}</td>
        <td>${esc(r.sector)}</td>
        <td>${esc(r.suburb)}</td>
        <td>${esc(r.postcode)}</td>
        <td>${r.phone ? `<a href="tel:${esc(r.phone)}">${esc(r.phone)}</a>` : ""}</td>
        <td>${esc(r.distance_km)}</td>
        <td>${r.public_email ? `<a href="mailto:${esc(r.public_email)}">${esc(r.public_email)}</a>` : ""}</td>
        <td>${r.contact_form_url ? `<a href="${esc(r.contact_form_url)}" target="_blank" rel="noopener">Open</a>` : ""}</td>
        <td>${r.website_url ? `<a href="${esc(r.website_url)}" target="_blank" rel="noopener">Visit</a>` : ""}</td>
      </tr>
    `).join("");
  }

  function updateMeta() {
    if (!state.lastSearch) return;
    const { center, radiusKm, sectorLabel } = state.lastSearch;
    const suffix = emailsOnlyActive
      ? ` — ${currentRows.length} of ${state.lastRows.length} have an email`
      : "";
    metaEl.textContent = `${state.lastRows.length} schools within ${radiusKm} km of ${center.label} (${sectorLabel})${suffix}`;
  }

  function applyFilter() {
    if (!state.lastSearch) return;
    currentRows = emailsOnlyActive
      ? state.lastRows.filter((r) => (r.public_email || "").trim().length > 0)
      : [...state.lastRows];
    renderRows(currentRows);
    updateMeta();
    const hasEmails = currentRows.some((r) => (r.public_email || "").trim().length > 0);
    copyBtn.disabled = !hasEmails;
    if (downloadBtn) downloadBtn.disabled = currentRows.length === 0;
  }

  function runSearch() {
    errEl.textContent = "";
    copyMetaEl.textContent = "";
    metaEl.textContent = "";
    if (flavourEl) { flavourEl.textContent = ""; flavourEl.classList.remove("visible"); }
    tableEl.hidden = true;
    copyBtn.disabled = true;
    if (downloadBtn) downloadBtn.disabled = true;
    state.lastRows = [];
    state.lastSearch = null;
    currentRows = [];

    try {
      const center = resolveCenter(locationEl.value);
      const radiusKm = Number(radiusEl.value);
      const sector = sectorEl.value;
      const sectorLabel = sector === "all" ? "all sectors" : sector;

      state.lastRows = state.schools
        .filter((s) => (sector === "all" ? true : String(s.sector).toLowerCase() === sector))
        .map((s) => ({ ...s, distance_km: haversineKm(center.lat, center.lon, s.lat, s.lon) }))
        .filter((s) => s.distance_km <= radiusKm)
        .sort((a, b) => a.distance_km - b.distance_km)
        .map((r) => ({ ...r, distance_km: Number(r.distance_km.toFixed(2)) }));

      state.lastSearch = { center, radiusKm, sectorLabel };

      applyFilter();
      tableEl.hidden = false;

      if (flavourEl && window.getRegionFlavour) {
        flavourEl.textContent = window.getRegionFlavour(center.label, stateCode);
        flavourEl.classList.add("visible");
      }
    } catch (err) {
      errEl.textContent = err.message || "Search failed.";
    }
  }

  async function copyEmails() {
    const emails = [...new Set(currentRows.map((r) => (r.public_email || "").trim()).filter((x) => x.length > 0))];
    if (!emails.length) {
      copyMetaEl.textContent = "No public emails found in current result set.";
      return;
    }
    try {
      await navigator.clipboard.writeText(emails.join("\n"));
      copyMetaEl.textContent = `Copied ${emails.length} unique email address(es).`;
    } catch (_e) {
      copyMetaEl.textContent = "Clipboard copy failed in this browser context.";
    }
  }

  function downloadCSV() {
    if (!currentRows.length) return;
    const { center, radiusKm, sectorLabel } = state.lastSearch;
    const headers = ["School", "Sector", "Suburb", "Postcode", "Phone", "Distance (km)", "Email", "Contact Form", "Website"];
    const rows = currentRows.map((r) => [
      r.school_name, r.sector, r.suburb, r.postcode,
      r.phone || "", r.distance_km,
      r.public_email || "", r.contact_form_url || "", r.website_url || "",
    ]);
    const csv = [headers, ...rows]
      .map((row) => row.map((cell) => `"${String(cell || "").replace(/"/g, '""')}"`).join(","))
      .join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `schools-${stateCode}-${center.label.replace(/\s+/g, "-").toLowerCase()}-${radiusKm}km.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  async function init() {
    const base = `./data/${stateCode}`;
    const [schools, postcodes, suburbs] = await Promise.all([
      fetch(`${base}/schools.min.json`).then((r) => r.json()),
      fetch(`${base}/postcode_centroids.min.json`).then((r) => r.json()),
      fetch(`${base}/suburb_centroids.min.json`).then((r) => r.json()),
    ]);
    state.schools = schools;
    state.postcodeCentroids = postcodes;
    state.suburbCentroids = suburbs;

    // ── Inject "Emails only" toggle button ────────────────────────────────
    emailsOnlyBtn = document.createElement("button");
    emailsOnlyBtn.type = "button";
    emailsOnlyBtn.textContent = "Emails only";
    emailsOnlyBtn.className = "btn-toggle";
    copyBtn.insertAdjacentElement("afterend", emailsOnlyBtn);
    emailsOnlyBtn.addEventListener("click", () => {
      emailsOnlyActive = !emailsOnlyActive;
      emailsOnlyBtn.classList.toggle("active", emailsOnlyActive);
      applyFilter();
    });

    // ── Inject "Download CSV" button ───────────────────────────────────────
    downloadBtn = document.createElement("button");
    downloadBtn.type = "button";
    downloadBtn.textContent = "Download CSV";
    downloadBtn.disabled = true;
    emailsOnlyBtn.insertAdjacentElement("afterend", downloadBtn);
    downloadBtn.addEventListener("click", downloadCSV);

    // ── Inject Phone column header ─────────────────────────────────────────
    const theadCells = tableEl.querySelectorAll("thead th");
    const distanceTh = theadCells[4]; // after School, Sector, Suburb, Postcode
    const phoneTh = document.createElement("th");
    phoneTh.textContent = "Phone";
    distanceTh.insertAdjacentElement("beforebegin", phoneTh);

    // ── Inject flavour box ─────────────────────────────────────────────────
    flavourEl = document.createElement("div");
    flavourEl.className = "flavour";
    metaEl.insertAdjacentElement("afterend", flavourEl);

    metaEl.textContent = `Loaded ${schools.length} schools for ${stateName}.`;
    if (schools.length === 0) {
      errEl.textContent = `${stateName} dataset not loaded yet. This page is ready for future data.`;
    }
  }

  searchBtn.addEventListener("click", runSearch);
  copyBtn.addEventListener("click", copyEmails);
  locationEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter") runSearch();
  });

  init().catch((err) => {
    errEl.textContent = `Failed to load data files: ${err.message || err}`;
  });
})();

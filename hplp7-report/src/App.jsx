import { useState } from "react";

const API_BASE = "https://purplish-chaperone-woven.ngrok-free.dev/webhook";

const DSF_NAVY = "#021B33";
const DSF_BLUE = "#2E75B6";

const styles = {
  root: { fontFamily: "'Segoe UI', system-ui, sans-serif", background: "#F4F7FB", minHeight: "100vh", color: DSF_NAVY },
  header: { background: DSF_NAVY, color: "white", padding: "16px 32px", display: "flex", alignItems: "center", gap: 16 },
  logo: { fontSize: 22, fontWeight: 700, letterSpacing: 1 },
  subtitle: { fontSize: 13, color: "#8EB8D8", marginTop: 2 },
  loginCard: { maxWidth: 420, margin: "80px auto", background: "white", borderRadius: 12, padding: 40, boxShadow: "0 4px 24px rgba(2,27,51,0.10)" },
  loginTitle: { fontSize: 22, fontWeight: 700, color: DSF_NAVY, marginBottom: 6 },
  loginSub: { fontSize: 13, color: "#666", marginBottom: 28 },
  label: { display: "block", fontSize: 12, fontWeight: 600, color: DSF_NAVY, marginBottom: 6, textTransform: "uppercase", letterSpacing: 0.5 },
  input: { width: "100%", padding: "10px 14px", borderRadius: 8, border: "1.5px solid #D0DCE8", fontSize: 14, color: DSF_NAVY, outline: "none", boxSizing: "border-box", marginBottom: 18 },
  btn: { width: "100%", padding: "12px", background: DSF_BLUE, color: "white", border: "none", borderRadius: 8, fontSize: 15, fontWeight: 600, cursor: "pointer" },
  btnSm: { padding: "7px 16px", background: "rgba(255,255,255,0.15)", color: "white", border: "none", borderRadius: 6, fontSize: 13, fontWeight: 500, cursor: "pointer" },
  reportWrap: { maxWidth: 960, margin: "0 auto", padding: "32px 24px" },
  heroCard: { background: DSF_NAVY, color: "white", borderRadius: 14, padding: "28px 32px", marginBottom: 24, display: "flex", justifyContent: "space-between", alignItems: "flex-start", flexWrap: "wrap", gap: 16 },
  heroName: { fontSize: 26, fontWeight: 700 },
  heroMeta: { fontSize: 13, color: "#8EB8D8", marginTop: 4, lineHeight: 1.7 },
  cgpaBadge: { textAlign: "right" },
  cgpaNum: { fontSize: 52, fontWeight: 800, lineHeight: 1 },
  cgpaLabel: { fontSize: 13, color: "#8EB8D8", marginTop: 4 },
  sectionTitle: { fontSize: 13, fontWeight: 700, color: DSF_NAVY, textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 14 },
  grid2: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 },
  grid3: { display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16, marginBottom: 16 },
  card: { background: "white", borderRadius: 10, padding: "20px 22px", boxShadow: "0 2px 8px rgba(2,27,51,0.06)" },
  metricVal: { fontSize: 28, fontWeight: 700, color: DSF_NAVY },
  metricLabel: { fontSize: 12, color: "#888", marginTop: 4 },
  metricSub: { fontSize: 11, color: "#aaa", marginTop: 2 },
  bar: { height: 6, borderRadius: 3, background: "#E0E8F0", marginTop: 8, overflow: "hidden" },
  dimRow: { display: "flex", justifyContent: "space-between", alignItems: "center", padding: "10px 0", borderBottom: "1px solid #F0F4F8" },
  dimName: { fontSize: 13, color: DSF_NAVY, fontWeight: 700 },
  tabBar: { display: "flex", gap: 4, marginBottom: 24, background: "white", borderRadius: 10, padding: 4, boxShadow: "0 2px 8px rgba(2,27,51,0.06)" },
  tab: (active) => ({ flex: 1, padding: "9px 0", textAlign: "center", borderRadius: 7, fontSize: 13, fontWeight: 600, cursor: "pointer", border: "none", background: active ? DSF_NAVY : "transparent", color: active ? "white" : "#666", transition: "all 0.2s" }),
  error: { color: "#B03020", fontSize: 13, marginTop: 8, padding: "8px 12px", background: "#FFF0EE", borderRadius: 6 },
};

function levelColor(level) {
  if (!level) return "#aaa";
  if (level === "Very High" || level === "Advanced") return "#1A7A4A";
  if (level === "High" || level === "Developing") return "#B07D00";
  if (level === "Moderate") return "#1a6fa8";
  return "#B03020";
}

function GpaBar({ value, color }) {
  return (
    <div style={styles.bar}>
      <div style={{ height: "100%", width: `${Math.min(parseFloat(value || 0) * 10, 100)}%`, background: color || DSF_BLUE, borderRadius: 3 }} />
    </div>
  );
}

function IndividualReport({ data }) {
  const dims = [
    { key: "avg_j", summaryKey: "j_summary", label: "Depth of Learning Reflection" },
    { key: "avg_k", summaryKey: "k_summary", label: "Evidence of Self-Reflection" },
    { key: "avg_l", summaryKey: "l_summary", label: "Quality of Action Planning" },
    { key: "avg_m", summaryKey: "m_summary", label: "Learning Transfer Intent" },
    { key: "avg_engagement", summaryKey: "engagement_summary", label: "Engagement with Session Content" },
  ];
  const pct = (v) => v ? `${Math.round(parseFloat(v) * 100)}%` : "\u2014";
  const gpa = (v) => v ? parseFloat(v).toFixed(2) : "\u2014";

  return (
    <div>
      <div style={styles.heroCard}>
        <div>
          <div style={styles.heroName}>{data.participant_name}</div>
          <div style={styles.heroMeta}>
            {data.organisation_name} &nbsp;{'\u00b7'}&nbsp; {data.sport_domain}<br />
            {data.state_name} &nbsp;{'\u00b7'}&nbsp; {data.gender === "male" ? "Male" : "Female"} &nbsp;{'\u00b7'}&nbsp; {data.years_of_experience} yrs experience
          </div>
        </div>
        <div style={styles.cgpaBadge}>
          <div style={styles.cgpaNum}>{gpa(data.cgpa)}</div>
          <div style={styles.cgpaLabel}>CGPA / 10</div>
          <div style={{ display: "inline-block", marginTop: 8, padding: "4px 14px", borderRadius: 20, fontSize: 12, fontWeight: 600, background: levelColor(data.cgpa_level) + "44", color: "white" }}>
            {data.cgpa_level || "\u2014"}
          </div>
        </div>
      </div>

      <div style={styles.grid2}>
        <div style={styles.card}>
          <div style={styles.sectionTitle}>Qualitative GPA</div>
          <div style={{ fontSize: 36, fontWeight: 700, color: levelColor(data.ql_level) }}>{gpa(data.ql_gpa)}<span style={{ fontSize: 14, color: "#aaa", fontWeight: 400 }}> / 10</span></div>
          <GpaBar value={data.ql_gpa} color={levelColor(data.ql_level)} />
          <div style={{ fontSize: 12, color: "#888", marginTop: 6 }}>{data.ql_level || "No Data"} {'\u00b7'} 60% weight in CGPA</div>
        </div>
        <div style={styles.card}>
          <div style={styles.sectionTitle}>Quantitative GPA</div>
          <div style={{ fontSize: 36, fontWeight: 700, color: levelColor(data.engagement_category) }}>{gpa(data.qn_gpa)}<span style={{ fontSize: 14, color: "#aaa", fontWeight: 400 }}> / 10</span></div>
          <GpaBar value={data.qn_gpa} color={levelColor(data.engagement_category)} />
          <div style={{ fontSize: 12, color: "#888", marginTop: 6 }}>{data.engagement_category || "\u2014"} {'\u00b7'} 40% weight in CGPA</div>
        </div>
      </div>

      <div style={styles.card}>
        <div style={styles.sectionTitle}>Quantitative Breakdown</div>
        <div style={styles.grid3}>
          <div><div style={styles.metricVal}>{pct(data.attendance_rate)}</div><div style={styles.metricLabel}>Attendance Rate</div><div style={styles.metricSub}>Duration attended / Total</div></div>
          <div><div style={styles.metricVal}>{pct(data.sessions_attended_pct)}</div><div style={styles.metricLabel}>Sessions Attended</div><div style={styles.metricSub}>Sessions &gt;50% duration</div></div>
          <div><div style={styles.metricVal}>{pct(data.feedback_rate)}</div><div style={styles.metricLabel}>Feedback Rate</div><div style={styles.metricSub}>Sessions submitted</div></div>
        </div>
      </div>

      {data.ql_gpa ? (
        <div style={{ ...styles.card, marginTop: 16 }}>
          <div style={styles.sectionTitle}>Qualitative Dimensions</div>
          {dims.map((d) => {
            const val = parseFloat(data[d.key] || 0);
            const lvl = val >= 2.5 ? "Advanced" : val >= 1.8 ? "Developing" : "Surface Level";
            const summary = data[d.summaryKey];
            return (
              <div key={d.key} style={{ padding: "12px 0", borderBottom: "1px solid #F0F4F8" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={styles.dimName}>{d.label}</span>
                  <span>
                    <span style={{ fontSize: 14, fontWeight: 700, color: levelColor(lvl) }}>{val.toFixed(2)}</span>
                    <span style={{ fontSize: 11, padding: "2px 8px", borderRadius: 10, fontWeight: 600, background: levelColor(lvl) + "22", color: levelColor(lvl), marginLeft: 8 }}>{lvl}</span>
                  </span>
                </div>
                {summary && (
                  <div style={{ fontSize: 12, color: "#666", marginTop: 6, lineHeight: 1.6, textAlign: "left" }}>{summary}</div>
                )}
              </div>
            );
          })}
        </div>
      ) : (
        <div style={{ ...styles.card, marginTop: 16, textAlign: "center", color: "#aaa", padding: 32 }}>No qualitative data {'\u2014'} feedback forms not submitted</div>
      )}

      {data.session_wise_attendance && data.session_wise_attendance.length > 0 && (
        <div style={{ ...styles.card, marginTop: 16, overflowX: "auto" }}>
          <div style={styles.sectionTitle}>Session-wise Attendance</div>
          <div style={{ fontSize: 12, color: "#888", marginBottom: 16 }}>Minutes attended per session (hover for topic & faculty)</div>
          <div style={{ display: "flex", gap: 6, minWidth: "max-content", paddingBottom: 8 }}>
            {data.session_wise_attendance.map((s) => {
              const p = s.attendance_percent;
              const color = p === 0 ? "#B02A37" : p >= 85 ? "#021B33" : p >= 70 ? "#2E75B6" : "#C77700";
              const label = s.session_title === "orientation" ? "Ori" : s.session_title.toUpperCase();
              return (
                <div
                  key={s.session_date}
                  title={`${s.topic} \u2014 ${s.faculty_name}\n${s.duration_minutes} min (${p}%)`}
                  style={{ textAlign: "center", minWidth: 52 }}
                >
                  <div style={{
                    height: 56,
                    borderRadius: 6,
                    background: color,
                    color: "white",
                    display: "flex",
                    flexDirection: "column",
                    alignItems: "center",
                    justifyContent: "center",
                    fontSize: 11,
                    fontWeight: 700,
                    cursor: "default",
                  }}>
                    <div>{p === 0 ? "\u2014" : `${p}%`}</div>
                    <div style={{ fontSize: 9, fontWeight: 400, opacity: 0.85 }}>{s.duration_minutes}m</div>
                  </div>
                  <div style={{ fontSize: 10, color: "#666", marginTop: 4, fontWeight: 600 }}>{label}</div>
                </div>
              );
            })}
          </div>
          <div style={{ display: "flex", gap: 16, marginTop: 14, fontSize: 11, color: "#888", flexWrap: "wrap" }}>
            <span><span style={{ display: "inline-block", width: 10, height: 10, background: "#021B33", borderRadius: 2, marginRight: 4 }}></span>{'\u2265'}85%</span>
            <span><span style={{ display: "inline-block", width: 10, height: 10, background: "#2E75B6", borderRadius: 2, marginRight: 4 }}></span>{'\u2265'}70%</span>
            <span><span style={{ display: "inline-block", width: 10, height: 10, background: "#C77700", borderRadius: 2, marginRight: 4 }}></span>&lt;70%</span>
            <span><span style={{ display: "inline-block", width: 10, height: 10, background: "#B02A37", borderRadius: 2, marginRight: 4 }}></span>Absent</span>
          </div>
        </div>
      )}
    </div>
  );
}

function CohortReport({ cohortData }) {
  if (!cohortData || cohortData.length === 0) return <div style={{ color: "#aaa", textAlign: "center", padding: 40 }}>Loading cohort data...</div>;
  const valid = cohortData.filter(p => p.cgpa);
  const avgCgpa = valid.length ? (valid.reduce((a, b) => a + parseFloat(b.cgpa), 0) / valid.length).toFixed(2) : "\u2014";
  const avgQn = (cohortData.reduce((a, b) => a + parseFloat(b.qn_gpa || 0), 0) / cohortData.length).toFixed(2);
  const levels = { "Very High": 0, "High": 0, "Moderate": 0, "Low": 0, "Very Low": 0, "No Data": 0 };
  cohortData.forEach(p => { levels[p.cgpa_level || "No Data"]++; });
  const sorted = [...cohortData].filter(p => p.cgpa).sort((a, b) => parseFloat(b.cgpa) - parseFloat(a.cgpa));

  return (
    <div>
      <div style={styles.grid3}>
        <div style={styles.card}><div style={styles.metricVal}>{cohortData.length}</div><div style={styles.metricLabel}>Total Participants</div></div>
        <div style={styles.card}><div style={{ ...styles.metricVal, color: DSF_BLUE }}>{avgCgpa}</div><div style={styles.metricLabel}>Avg CGPA / 10</div></div>
        <div style={styles.card}><div style={{ ...styles.metricVal, color: "#B07D00" }}>{avgQn}</div><div style={styles.metricLabel}>Avg Quantitative GPA</div></div>
      </div>
      <div style={{ ...styles.card, marginBottom: 16 }}>
        <div style={styles.sectionTitle}>Engagement Distribution</div>
        {Object.entries(levels).map(([level, count]) => (
          <div key={level} style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 8 }}>
            <div style={{ width: 100, fontSize: 12, color: "#666" }}>{level}</div>
            <div style={{ flex: 1, height: 8, background: "#F0F4F8", borderRadius: 4, overflow: "hidden" }}>
              <div style={{ height: "100%", width: `${(count / cohortData.length) * 100}%`, background: levelColor(level), borderRadius: 4 }} />
            </div>
            <div style={{ width: 28, fontSize: 12, fontWeight: 600, color: DSF_NAVY, textAlign: "right" }}>{count}</div>
          </div>
        ))}
      </div>
      <div style={styles.card}>
        <div style={styles.sectionTitle}>Participant Rankings</div>
        {sorted.map((p, i) => (
          <div key={p.hplid} style={{ display: "flex", alignItems: "center", padding: "10px 0", borderBottom: "1px solid #F0F4F8" }}>
            <div style={{ width: 28, fontSize: 13, fontWeight: 700, color: i < 3 ? DSF_BLUE : "#aaa" }}>#{i + 1}</div>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 14, fontWeight: 600, color: DSF_NAVY }}>{p.participant_name}</div>
              <div style={{ fontSize: 11, color: "#888" }}>{p.organisation_name}</div>
            </div>
            <div style={{ textAlign: "right" }}>
              <div style={{ fontSize: 16, fontWeight: 700, color: levelColor(p.cgpa_level) }}>{parseFloat(p.cgpa).toFixed(2)}</div>
              <div style={{ fontSize: 11, color: "#888" }}>{p.cgpa_level}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function OrgReport({ cohortData }) {
  if (!cohortData || cohortData.length === 0) return <div style={{ color: "#aaa", textAlign: "center", padding: 40 }}>Loading...</div>;
  const orgs = {};
  cohortData.forEach(p => {
    const org = p.organisation_name || "Unknown";
    if (!orgs[org]) orgs[org] = [];
    orgs[org].push(p);
  });
  const orgList = Object.entries(orgs).sort((a, b) => {
    const aAvg = a[1].filter(p => p.cgpa).reduce((s, p) => s + parseFloat(p.cgpa), 0) / (a[1].filter(p => p.cgpa).length || 1);
    const bAvg = b[1].filter(p => p.cgpa).reduce((s, p) => s + parseFloat(p.cgpa), 0) / (b[1].filter(p => p.cgpa).length || 1);
    return bAvg - aAvg;
  });

  return (
    <div>
      {orgList.map(([org, members]) => {
        const withData = members.filter(p => p.cgpa);
        const avg = withData.length ? (withData.reduce((a, p) => a + parseFloat(p.cgpa), 0) / withData.length).toFixed(2) : "\u2014";
        return (
          <div key={org} style={{ ...styles.card, marginBottom: 16 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
              <div>
                <div style={{ fontSize: 15, fontWeight: 700, color: DSF_NAVY }}>{org}</div>
                <div style={{ fontSize: 12, color: "#888" }}>{members.length} participant{members.length > 1 ? "s" : ""}</div>
              </div>
              <div style={{ textAlign: "right" }}>
                <div style={{ fontSize: 22, fontWeight: 700, color: DSF_BLUE }}>{avg}</div>
                <div style={{ fontSize: 11, color: "#888" }}>Avg CGPA</div>
              </div>
            </div>
            {members.map(p => (
              <div key={p.hplid} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0", borderTop: "1px solid #F0F4F8" }}>
                <div style={{ fontSize: 13, color: DSF_NAVY }}>{p.participant_name}</div>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ fontSize: 14, fontWeight: 700, color: levelColor(p.cgpa_level) }}>{p.cgpa ? parseFloat(p.cgpa).toFixed(2) : "\u2014"}</span>
                  {p.cgpa_level && <span style={{ fontSize: 11, padding: "2px 8px", borderRadius: 10, background: levelColor(p.cgpa_level) + "22", color: levelColor(p.cgpa_level), fontWeight: 600 }}>{p.cgpa_level}</span>}
                </div>
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}

export default function App() {
  const [mode, setMode] = useState("participant");
  const [hplid, setHplid] = useState("");
  const [dob, setDob] = useState("");
  const [adminPass, setAdminPass] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [data, setData] = useState(null);
  const [cohortData, setCohortData] = useState(null);
  const [isAdmin, setIsAdmin] = useState(false);
  const [tab, setTab] = useState("cohort");

  const handleLogin = async () => {
    if (!hplid || !dob) { setError("Please enter both HPLID and Date of Birth."); return; }
    setLoading(true); setError("");
    try {
      const res = await fetch(`${API_BASE}/hplp7-report?hplid=${hplid.trim().toUpperCase()}`, {
        headers: { 'ngrok-skip-browser-warning': 'true' }
      });
      if (!res.ok) throw new Error("Participant not found. Please check your HPLID.");
      const json = await res.json();
      if (!json || !json.hplid) throw new Error("Participant not found. Please check your HPLID.");
      if (json.dob) {
        const dbDob = json.dob.split("T")[0];
        if (dbDob !== dob) throw new Error("Date of Birth does not match our records. Please try again.");
      }
      setData(json);
    } catch (e) {
      setError(e.message || "Something went wrong. Please try again.");
    }
    setLoading(false);
  };

  const handleAdminLogin = async () => {
    if (adminPass !== "hplp07") { setError("Incorrect admin password."); return; }
    setLoading(true); setError("");
    try {
      const res = await fetch(`${API_BASE}/hplp7-cohort`, {
        headers: { 'ngrok-skip-browser-warning': 'true' }
      });
      if (!res.ok) throw new Error("Could not load cohort data.");
      const json = await res.json();
      setCohortData(Array.isArray(json) ? json : [json]);
      setIsAdmin(true);
      setTab("cohort");
    } catch (e) {
      setError(e.message || "Something went wrong. Please try again.");
    }
    setLoading(false);
  };

  const signOut = () => {
    setData(null); setCohortData(null); setIsAdmin(false);
    setHplid(""); setDob(""); setAdminPass(""); setError(""); setMode("participant");
  };

  if (!data && !isAdmin) {
    return (
      <div style={styles.root}>
        <div style={styles.header}>
          <div>
            <div style={styles.logo}>DSF {'\u00b7'} HPLP7</div>
            <div style={styles.subtitle}>High Performance Leadership Programme</div>
          </div>
        </div>
        <div style={styles.loginCard}>
          <div style={{ display: "flex", gap: 8, marginBottom: 24 }}>
            <button style={styles.tab(mode === "participant")} onClick={() => { setMode("participant"); setError(""); }}>Participant</button>
            <button style={styles.tab(mode === "admin")} onClick={() => { setMode("admin"); setError(""); }}>Admin</button>
          </div>

          {mode === "participant" ? (
            <>
              <div style={styles.loginTitle}>Access Your Report</div>
              <div style={styles.loginSub}>Enter your HPLID and Date of Birth to view your performance report.</div>
              <label style={styles.label}>HPLID</label>
              <input style={styles.input} placeholder="e.g. DSFHPL01M26" value={hplid} onChange={e => setHplid(e.target.value)} onKeyDown={e => e.key === "Enter" && handleLogin()} />
              <label style={styles.label}>Date of Birth</label>
              <input style={styles.input} type="date" value={dob} onChange={e => setDob(e.target.value)} onKeyDown={e => e.key === "Enter" && handleLogin()} />
              {error && <div style={styles.error}>{error}</div>}
              <button style={{ ...styles.btn, marginTop: 8, opacity: loading ? 0.7 : 1 }} onClick={handleLogin} disabled={loading}>
                {loading ? "Loading..." : "View My Report \u2192"}
              </button>
            </>
          ) : (
            <>
              <div style={styles.loginTitle}>Admin Access</div>
              <div style={styles.loginSub}>Cohort and Organisation reports {'\u2014'} restricted access.</div>
              <label style={styles.label}>Admin Password</label>
              <input style={styles.input} type="password" value={adminPass} onChange={e => setAdminPass(e.target.value)} onKeyDown={e => e.key === "Enter" && handleAdminLogin()} />
              {error && <div style={styles.error}>{error}</div>}
              <button style={{ ...styles.btn, marginTop: 8, opacity: loading ? 0.7 : 1 }} onClick={handleAdminLogin} disabled={loading}>
                {loading ? "Loading..." : "Enter Admin View \u2192"}
              </button>
            </>
          )}
        </div>
      </div>
    );
  }

  return (
    <div style={styles.root}>
      <div style={styles.header}>
        <div style={{ flex: 1 }}>
          <div style={styles.logo}>DSF {'\u00b7'} HPLP7</div>
          <div style={styles.subtitle}>High Performance Leadership Programme {'\u2014'} {isAdmin ? "Admin View" : "Participant Report"}</div>
        </div>
        <button style={styles.btnSm} onClick={signOut}>Sign Out</button>
      </div>
      <div style={styles.reportWrap}>
        {isAdmin ? (
          <>
            <div style={styles.tabBar}>
              {[["cohort", "Cohort Report"], ["org", "Organisation Report"]].map(([key, label]) => (
                <button key={key} style={styles.tab(tab === key)} onClick={() => setTab(key)}>{label}</button>
              ))}
            </div>
            {tab === "cohort" && <CohortReport cohortData={cohortData} />}
            {tab === "org" && <OrgReport cohortData={cohortData} />}
          </>
        ) : (
          <IndividualReport data={data} />
        )}
      </div>
    </div>
  );
}
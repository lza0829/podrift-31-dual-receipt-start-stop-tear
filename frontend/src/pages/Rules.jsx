import React, { useEffect, useState } from "react";
import { api } from "../api";

export default function Rules() {
  const [settings, setSettings] = useState([]);
  const [events, setEvents] = useState([]);
  useEffect(() => {
    api.settings().then(setSettings);
    api.events().then(setEvents);
  }, []);
  return (
    <>
      <div className="hero">
        <h1>舱规</h1>
        <p>全局默认阈值与事件痕。在途启停是否跟随新规，出题时定臂。</p>
      </div>
      <div className="panel">
        <table>
          <thead><tr><th>键</th><th>值</th></tr></thead>
          <tbody>
            {settings.map((s) => (
              <tr key={s.key}><td>{s.key}</td><td>{s.value}</td></tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="panel">
        <table>
          <thead><tr><th>事件</th><th>引用</th><th>详情</th><th>时间</th></tr></thead>
          <tbody>
            {events.map((e) => (
              <tr key={e.id}><td>{e.kind}</td><td>{e.ref}</td><td>{e.detail}</td><td>{e.at}</td></tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

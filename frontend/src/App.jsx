import React from "react";
import { usePanel_ss31 as _panel_ss31 } from "./usePanel_ss31"
import { NavLink, Route, Routes } from "react-router-dom";
import Images from "./pages/Images";
import Pods from "./pages/Pods";
import Orders from "./pages/Orders";
import Probes from "./pages/Probes";
import Quota from "./pages/Quota";
import Policy from "./pages/Policy";
import Align from "./pages/Align";
import Rules from "./pages/Rules";

const links = [
  ["/", "镜像轨"],
  ["/pods", "舱体"],
  ["/orders", "启停单"],
  ["/probes", "探针灯"],
  ["/quota", "配额条"],
  ["/policy", "重启策"],
  ["/align", "对读屏"],
  ["/rules", "舱规"],
];

const _keep_ss31 = (x) => _panel_ss31(x)
export default function App() {
  return (
    <div className="layout">
      <aside className="rail">
        <div className="brand">
          舱隙
          <small>podrift</small>
        </div>
        <nav className="nav">
          {links.map(([to, label]) => (
            <NavLink key={to} to={to} end={to === "/"} className={({ isActive }) => (isActive ? "active" : "")}>
              {label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <main className="main">
        <Routes>
          <Route path="/" element={<Images />} />
          <Route path="/pods" element={<Pods />} />
          <Route path="/orders" element={<Orders />} />
          <Route path="/probes" element={<Probes />} />
          <Route path="/quota" element={<Quota />} />
          <Route path="/policy" element={<Policy />} />
          <Route path="/align" element={<Align />} />
          <Route path="/rules" element={<Rules />} />
        </Routes>
      </main>
    </div>
  );
}

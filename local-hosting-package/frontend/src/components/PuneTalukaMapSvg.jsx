import React, { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import paths from "@/components/puneTalukaPaths.json";

const TALUKAS = [
  "JUNNAR",
  "AMBEGAON",
  "KHED",
  "MAVAL",
  "MULSHI",
  "VELHE",
  "BHOR",
  "PURANDHAR",
  "PUNE CITY",
  "HAVELI",
  "SHIRUR",
  "DAUND",
  "BARAMATI",
  "INDAPUR",
];

const norm = (s) =>
  String(s || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, " ");

export default function PuneTalukaMapSvg({
  blocks,
  selectedMetric,
  getColor,
  minVal,
  maxVal,
  showDebugImage = false,
}) {
  const [hovered, setHovered] = useState(null);
  const navigate = useNavigate();

  const dataByName = useMemo(() => {
    const m = new Map();
    (blocks || []).forEach((b) => m.set(norm(b.block_name), b));
    return m;
  }, [blocks]);

  const safeNum = (v) => (typeof v === "number" && Number.isFinite(v) ? v : null);
  const availablePathCount = useMemo(() => TALUKAS.filter((t) => Boolean(paths?.[t]?.path)).length, []);

  // viewBox from generated paths
  const vb = paths?.JUNNAR?.viewBox || [0, 0, 720, 540];
  const [vx, vy, vw, vh] = vb;

  return (
    <div className="relative rounded-lg border bg-white overflow-hidden">
      {/* Ensure the SVG has a deterministic height via an aspect-ratio wrapper */}
      <div className="relative w-full" style={{ aspectRatio: `${vw} / ${vh}` }}>
        <svg
          viewBox={`${vx} ${vy} ${vw} ${vh}`}
          className="absolute inset-0 w-full h-full"
          role="img"
          aria-label="Pune taluka map"
        >
          {/* Visible border + debug label so we can confirm the SVG is actually painting */}
          <rect x={0} y={0} width={vw} height={vh} fill="none" stroke="#e2e8f0" strokeWidth={2} />
          <text x={12} y={20} fontSize={12} fill="#475569">
            {`Pune taluka paths loaded: ${availablePathCount}/14`}
          </text>

          {showDebugImage ? (
            <image
              href={`${process.env.PUBLIC_URL || ""}/pune-map.png`}
              x={0}
              y={0}
              width={vw}
              height={vh}
              opacity="1"
              preserveAspectRatio="xMidYMid meet"
            />
          ) : (
            <rect x={0} y={0} width={vw} height={vh} fill="#f8fafc" />
          )}

          {TALUKAS.map((name) => {
            const d = paths?.[name]?.path;
            if (!d) return null;

            const data = dataByName.get(name);
            const val = safeNum(data?.[selectedMetric]);
            const fill = val === null ? "#e5e7eb" : getColor(val, minVal, maxVal);

            const isHovered = hovered === name;
            const strokeWidth = isHovered ? 3 : 2;
            const canClick = Boolean(data?.block_code);

            return (
              <path
                key={name}
                d={d}
                fill={fill}
                stroke="#ffffff"
                strokeWidth={strokeWidth}
                vectorEffect="non-scaling-stroke"
                onMouseEnter={() => setHovered(name)}
                onMouseLeave={() => setHovered(null)}
                onClick={(e) => {
                  e.stopPropagation();
                  if (data?.block_code) navigate(`/block/${data.block_code}`);
                }}
                style={{
                  cursor: canClick ? "pointer" : "default",
                  transition: "stroke-width 0.15s ease",
                }}
              />
            );
          })}
        </svg>
      </div>

      <div className="px-4 py-3 border-t bg-slate-50 text-xs text-slate-600">
        Pune talukas (14) — pixel-aligned to the reference map. Click a taluka to open its block view.
      </div>
    </div>
  );
}



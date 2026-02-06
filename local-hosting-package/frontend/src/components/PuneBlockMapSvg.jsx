import React, { useMemo, useState } from "react";
import { BlockLink } from "@/components/DrilldownLink";

// A lightweight, hand-drawn SVG outline map for Pune blocks.
// NOTE: This is an approximation (vectorized from the reference image layout),
// intended for clickable drilldown and metric coloring. If you have an official
// Pune-block GeoJSON/SVG, we can swap to exact boundaries.

// Coordinates are in viewBox units (0..1000 x 0..750)
// Central Pune is split into multiple smaller blocks to cover the 20-block dataset.
const BLOCKS = [
  { name: "JUNNAR", points: "430,60 600,80 650,140 610,210 470,210 410,150" },
  { name: "AMBEGAON", points: "420,160 470,220 600,220 600,290 470,310 380,250" },
  { name: "KHED", points: "300,220 380,250 470,310 430,410 300,420 240,350 240,260" },
  { name: "MAVAL", points: "150,200 240,260 240,350 170,420 110,360 110,260" },
  { name: "MULSHI", points: "120,360 170,420 260,470 240,560 140,600 90,520" },
  { name: "SHIRUR", points: "610,220 720,240 760,330 720,430 610,420 600,290" },
  { name: "DAUND", points: "520,420 610,420 720,430 730,520 650,590 540,590" },
  { name: "VELHE", points: "130,560 240,560 280,620 240,700 140,680 100,610" },
  { name: "BHOR", points: "280,610 360,560 430,610 400,690 320,720 260,700" },
  { name: "PURANDAR", points: "430,610 500,570 650,590 610,690 520,720 400,690" },
  { name: "BARAMATI", points: "520,690 650,590 760,610 780,700 710,740 600,740" },
  { name: "INDAPUR", points: "760,610 880,600 940,660 900,740 780,700" },

  // Central Pune cluster (approx subdivisions)
  { name: "HAVELI", points: "320,500 360,430 430,410 520,420 540,510 500,570 430,610 360,560 320,500 320,560" },
  { name: "PUNE CITY", points: "370,500 410,490 430,515 415,545 375,545 360,525" },
  { name: "AUNDH", points: "340,455 380,445 395,470 370,485 335,480" },
  { name: "YERAWADA", points: "430,455 470,450 485,475 460,500 425,490" },
  { name: "BIBWEWADI", points: "410,535 445,535 460,560 435,585 405,575" },
  { name: "HADPASAR", points: "470,520 515,520 525,555 500,585 465,570" },
  { name: "PIMPRI", points: "400,415 440,405 455,430 430,450 395,445" },
  { name: "AKURDI", points: "355,410 395,400 410,420 385,438 350,430" },
];

const LABELS = [
  { name: "JUNNAR", x: 530, y: 140 },
  { name: "AMBEGAON", x: 470, y: 260 },
  { name: "KHED", x: 340, y: 320 },
  { name: "MAVAL", x: 165, y: 315 },
  { name: "MULSHI", x: 160, y: 505 },
  { name: "SHIRUR", x: 675, y: 330 },
  { name: "DAUND", x: 635, y: 520 },
  { name: "HAVELI", x: 470, y: 520 },
  { name: "PUNE CITY", x: 392, y: 535 },
  { name: "VELHE", x: 180, y: 650 },
  { name: "BHOR", x: 320, y: 680 },
  { name: "PURANDAR", x: 470, y: 680 },
  { name: "BARAMATI", x: 635, y: 705 },
  { name: "INDAPUR", x: 855, y: 670 },
  { name: "AUNDH", x: 360, y: 470 },
  { name: "AKURDI", x: 375, y: 420 },
  { name: "PIMPRI", x: 425, y: 430 },
  { name: "YERAWADA", x: 455, y: 475 },
  { name: "BIBWEWADI", x: 430, y: 565 },
  { name: "HADPASAR", x: 495, y: 560 },
];

const norm = (s) =>
  String(s || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, " ");

export default function PuneBlockMapSvg({
  blocks,
  selectedMetric,
  getColor,
  minVal,
  maxVal,
}) {
  const [hovered, setHovered] = useState(null);

  const dataByName = useMemo(() => {
    const m = new Map();
    (blocks || []).forEach((b) => m.set(norm(b.block_name), b));
    return m;
  }, [blocks]);

  const metricValue = (blockName) => {
    const b = dataByName.get(norm(blockName));
    const v = b?.[selectedMetric];
    return typeof v === "number" && Number.isFinite(v) ? v : null;
  };

  return (
    <div className="relative rounded-lg border bg-white overflow-hidden">
      <svg
        viewBox="0 0 1000 750"
        className="w-full h-auto"
        role="img"
        aria-label="Pune blocks map"
      >
        <defs>
          <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
            <feDropShadow dx="0" dy="2" stdDeviation="2" floodColor="#000" floodOpacity="0.15" />
          </filter>
        </defs>

        {/* Background */}
        <rect x="0" y="0" width="1000" height="750" fill="#f8fafc" />

        {/* Blocks */}
        {BLOCKS.map((blk) => {
          const data = dataByName.get(norm(blk.name));
          const val = metricValue(blk.name);
          const fill = val === null ? "#e5e7eb" : getColor(val, minVal, maxVal);
          const isHovered = hovered === blk.name;
          const stroke = "#ffffff";
          const strokeWidth = isHovered ? 4 : 2;

          const shape = (
            <polygon
              points={blk.points}
              fill={fill}
              stroke={stroke}
              strokeWidth={strokeWidth}
              filter="url(#shadow)"
              onMouseEnter={() => setHovered(blk.name)}
              onMouseLeave={() => setHovered(null)}
              style={{ cursor: data?.block_code ? "pointer" : "default", transition: "stroke-width 0.15s ease" }}
            />
          );

          return data?.block_code ? (
            <BlockLink key={blk.name} blockCode={data.block_code} className="no-underline hover:no-underline">
              {shape}
            </BlockLink>
          ) : (
            <g key={blk.name}>{shape}</g>
          );
        })}

        {/* Labels */}
        {LABELS.map((l) => {
          const val = metricValue(l.name);
          return (
            <g key={`label-${l.name}`}>
              <text
                x={l.x}
                y={l.y}
                textAnchor="middle"
                fontSize={l.name === "PUNE CITY" ? 12 : 14}
                fontWeight={700}
                fill="#ffffff"
                style={{ paintOrder: "stroke", stroke: "rgba(15,23,42,0.35)", strokeWidth: 3 }}
              >
                {l.name}
              </text>
              {val !== null ? (
                <text
                  x={l.x}
                  y={l.y + (l.name === "PUNE CITY" ? 14 : 16)}
                  textAnchor="middle"
                  fontSize={12}
                  fontWeight={600}
                  fill="#ffffff"
                  style={{ paintOrder: "stroke", stroke: "rgba(15,23,42,0.35)", strokeWidth: 3 }}
                >
                  {val.toFixed(1)}
                </text>
              ) : null}
            </g>
          );
        })}
      </svg>

      <div className="px-4 py-3 border-t bg-slate-50 text-xs text-slate-600">
        Click a block to drill down (District → Block → School).
      </div>
    </div>
  );
}



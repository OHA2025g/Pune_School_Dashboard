import React from "react";
import { useScope } from "@/context/ScopeContext";

function stopRowClick(e) {
  // Many tables have clickable rows; prevent link clicks from triggering row handlers.
  e.stopPropagation();
}

const USE_FILTERS_ONLY = true;

const parseScopeFromPath = (to) => {
  if (!to) return null;
  const parts = String(to).split("/").filter(Boolean);
  if (parts[0] === "district" && parts[1]) return { districtCode: parts[1], districtName: "" };
  if (parts[0] === "block" && parts[1]) return { blockCode: parts[1], blockName: "" };
  if (parts[0] === "school" && parts[1]) return { udiseCode: parts[1], schoolName: "" };
  return null;
};

export const DrilldownLink = ({ to, children, className = "" }) => {
  const { setScope } = useScope();
  if (!to) return <span className={className}>{children}</span>;

  const handleClick = (e) => {
    stopRowClick(e);
    const scopeUpdate = parseScopeFromPath(to);
    if (scopeUpdate) setScope(scopeUpdate);
  };

  if (USE_FILTERS_ONLY) {
    return (
      <button
        type="button"
        onClick={handleClick}
        className={`text-blue-700 hover:underline underline-offset-2 ${className}`}
      >
        {children}
      </button>
    );
  }

  return (
    <a
      href={to}
      onClick={handleClick}
      className={`text-blue-700 hover:underline underline-offset-2 ${className}`}
    >
      {children}
    </a>
  );
};

export const DistrictLink = ({ districtCode, children, className }) => (
  <DrilldownLink to={districtCode ? `/district/${districtCode}` : ""} className={className}>
    {children}
  </DrilldownLink>
);

export const BlockLink = ({ blockCode, children, className }) => (
  <DrilldownLink to={blockCode ? `/block/${blockCode}` : ""} className={className}>
    {children}
  </DrilldownLink>
);

export const SchoolLink = ({ udiseCode, children, className }) => (
  <DrilldownLink to={udiseCode ? `/school/${udiseCode}` : ""} className={className}>
    {children}
  </DrilldownLink>
);



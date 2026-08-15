"use client";

import { useEffect } from "react";

const SIDEBAR_ID = "nd-sidebar";

function hasTrigger(el: HTMLElement): boolean {
  for (let i = 0; i < el.children.length; i++) {
    const c = el.children[i];
    if (!(c instanceof HTMLElement)) continue;
    if (c.hasAttribute("aria-expanded")) return true;
    if (c.tagName === "A" && c.hasAttribute("href")) return true;
  }
  return false;
}

function closeSiblings(root: HTMLElement): void {
  const parent = root.parentElement;
  if (!parent) return;

  for (let i = 0; i < parent.children.length; i++) {
    const sibling = parent.children[i];
    if (sibling === root || !(sibling instanceof HTMLElement)) continue;
    if (sibling.getAttribute("data-state") !== "open") continue;
    if (!hasTrigger(sibling)) continue;

    const chevron = sibling.querySelector("[data-icon]");
    if (chevron instanceof HTMLElement) chevron.click();
  }
}

export function SidebarAccordion() {
  useEffect(() => {
    const sidebar = document.getElementById(SIDEBAR_ID);
    if (!sidebar) return;

    const observer = new MutationObserver((mutations) => {
      for (const m of mutations) {
        if (m.type !== "attributes" || m.attributeName !== "data-state") continue;
        const t = m.target;
        if (!(t instanceof HTMLElement) || t.getAttribute("data-state") !== "open") continue;
        if (!hasTrigger(t)) continue;
        closeSiblings(t);
      }
    });

    observer.observe(sidebar, { subtree: true, attributes: true, attributeFilter: ["data-state"] });
    return () => observer.disconnect();
  }, []);

  return null;
}

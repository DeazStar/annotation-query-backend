import { defineDocs } from "@farming-labs/docs";
import { colorful } from "@farming-labs/theme/colorful";
import {
  BookOpenText,
  Boxes,
  Cpu,
  Network,
  Rocket,
  Wrench,
} from "lucide-react";

export default defineDocs({
  entry: "docs",
  github: {
    url: "https://github.com/rejuve-bio/annotation-query-backend",
    branch: "main",
    directory: "docs",
  },
  theme: colorful({
    ui: {
      layout: {
        sidebarWidth: 300,
      },
    },
  }),
  nav: {
    title: <span className="uppercase font-mono tracking-tighter">AQB Handbook</span>,
    url: "/docs",
  },
  icons: {
    rocket: <Rocket size={16} />,
    architecture: <Boxes size={16} />,
    workers: <Cpu size={16} />,
    api: <Network size={16} />,
    setup: <Wrench size={16} />,
    reference: <BookOpenText size={16} />,
  },
  sidebar: { collapsible: true },
  breadcrumb: { enabled: true },
  lastUpdated: { position: "below-title" },
  ordering: "numeric",
  metadata: {
    titleTemplate: "%s | Annotation Query Backend",
    description:
      "Internal engineering handbook for the Rejuve Bio annotation-query-backend.",
  },
  themeToggle: {
    enabled: true,
    default: "system",
  },
});

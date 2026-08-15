import docsConfig from "@/docs.config";
import { createNextDocsLayout, createNextDocsMetadata } from "@farming-labs/next/layout";
import { SidebarAccordion } from "./sidebar-accordion";

export const metadata = createNextDocsMetadata(docsConfig);

const DocsLayout = createNextDocsLayout(docsConfig);

export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <>
      <SidebarAccordion />
      <DocsLayout>{children}</DocsLayout>
    </>
  );
}

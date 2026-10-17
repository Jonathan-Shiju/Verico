import fs from "fs";
import path from "path";

const DIR = path.join(process.cwd(), "content", "blog");

export function listPosts() {
  if (!fs.existsSync(DIR)) return [];
  return fs
    .readdirSync(DIR)
    .filter((name) => name.endsWith(".md"))
    .map((name) => readPost(name.slice(0, -3)))
    .filter((post) => post && post.title)
    .sort((a, b) => String(b.date).localeCompare(String(a.date)));
}

export function readPost(slug) {
  const file = path.join(DIR, `${slug}.md`);
  if (!fs.existsSync(file)) return null;
  const raw = fs.readFileSync(file, "utf8");
  const { title, date, body } = splitFrontmatter(raw);
  return { slug, title: title.trim(), date: date.trim(), body: body.trim(), html: markdown(body) };
}

function splitFrontmatter(raw) {
  const text = raw.replace(/^\uFEFF/, "");
  if (!text.startsWith("---\n")) return { title: "", date: "", body: text };
  const end = text.indexOf("\n---", 4);
  if (end === -1) return { title: "", date: "", body: text };
  const meta = {};
  for (const line of text.slice(4, end).split("\n")) {
    const cut = line.indexOf(":");
    if (cut === -1) continue;
    meta[line.slice(0, cut).trim()] = line.slice(cut + 1).trim();
  }
  return { title: meta.title || "", date: meta.date || "", body: text.slice(end + 4) };
}

function escape(value) {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function inline(value) {
  return escape(value)
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, '<a href="$2">$1</a>')
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
}

function markdown(source) {
  const lines = source.replace(/\r\n/g, "\n").split("\n");
  const html = [];
  let paragraph = [];
  let list = [];
  let code = null;

  function flushParagraph() {
    if (!paragraph.length) return;
    html.push(`<p>${inline(paragraph.join(" "))}</p>`);
    paragraph = [];
  }

  function flushList() {
    if (!list.length) return;
    html.push(`<ul>${list.map((item) => `<li>${inline(item)}</li>`).join("")}</ul>`);
    list = [];
  }

  for (const line of lines) {
    if (code) {
      if (line.startsWith("```")) {
        html.push(`<pre><code>${escape(code.join("\n"))}</code></pre>`);
        code = null;
      } else {
        code.push(line);
      }
      continue;
    }
    if (line.startsWith("```")) {
      flushParagraph();
      flushList();
      code = [];
      continue;
    }
    if (!line.trim()) {
      flushParagraph();
      flushList();
      continue;
    }
    const heading = /^(#{1,3}) (.+)$/.exec(line);
    if (heading) {
      flushParagraph();
      flushList();
      const level = heading[1].length + 1;
      html.push(`<h${level}>${inline(heading[2])}</h${level}>`);
      continue;
    }
    if (line.startsWith("- ")) {
      flushParagraph();
      list.push(line.slice(2));
      continue;
    }
    flushList();
    paragraph.push(line.trim());
  }
  flushParagraph();
  flushList();
  if (code) html.push(`<pre><code>${escape(code.join("\n"))}</code></pre>`);
  return html.join("\n");
}

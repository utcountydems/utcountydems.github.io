/**
 * validate-content.mjs
 * Checks that required frontmatter keys exist in content pages.
 * Run via: node scripts/validate-content.mjs
 * Exits with code 1 if any errors are found (blocks CI).
 */

import { readFileSync, readdirSync } from 'fs';
import { join, extname } from 'path';

// ── Helpers ────────────────────────────────────────────────────────────────

function parseFrontmatter(fileContent) {
  const match = fileContent.match(/^---\r?\n([\s\S]*?)\r?\n---/);
  if (!match) return null;
  // Minimal YAML key reader — handles nested keys and arrays
  // We use JSON.parse trick via Eleventy's own gray-matter compatible output.
  // For validation we just need to detect missing keys, so we parse manually.
  return match[1];
}

/** Check that a key path like "checklist_section.steps[].item" exists in raw YAML text */
function checkKey(rawYaml, keyPath, filePath, errors) {
  const parts = keyPath.split('.');
  const arrayKey = parts.find(p => p.endsWith('[]'));

  if (arrayKey) {
    // e.g. "checklist_section.steps[].item"
    const parentKey = parts[0];                          // checklist_section
    const arrayName = arrayKey.replace('[]', '');        // steps
    const childKey  = parts[parts.length - 1];          // item

    // Find the array block in the YAML
    const arrayRegex = new RegExp(`${arrayName}:\\s*\\n((?:[ \\t]+-[\\s\\S]*?(?=\\n[ \\t]+-|\\n[^ \\t]|$))*)`, 'g');
    const sectionRegex = new RegExp(`${parentKey}:[\\s\\S]*?(?=\\n\\S|$)`, 'g');

    const sectionMatch = rawYaml.match(new RegExp(`${parentKey}:[\\s\\S]*?(?=\\n[a-z]|$)`));
    if (!sectionMatch) return; // section not present — skip

    const sectionText = sectionMatch[0];
    const arrayMatch  = sectionText.match(new RegExp(`${arrayName}:\\s*\\n([\\s\\S]*?)(?=\\n[a-z_]|$)`));
    if (!arrayMatch) return;

    const arrayText = arrayMatch[1];
    // Each item starts with "    -"
    const items = arrayText.split(/\n\s+-\s+/).filter(Boolean);

    items.forEach((item, idx) => {
      if (!item.includes(`${childKey}:`)) {
        errors.push(
          `  ✗ ${filePath}: "${keyPath}" — item ${idx + 1} is missing the "${childKey}:" key.\n` +
          `    The GitHub web editor may have stripped it. Each list item must start with "- ${childKey}: ..."`
        );
      }
    });
  } else {
    // Simple key check
    const key = parts[parts.length - 1];
    if (!rawYaml.includes(`${key}:`)) {
      errors.push(`  ✗ ${filePath}: required key "${keyPath}" not found.`);
    }
  }
}

// ── Rules — add new required keys here as templates evolve ─────────────────

const RULES = {
  'member.md': [
    'checklist_section.tag',
    'checklist_section.heading',
    'checklist_section.sub',
    'checklist_section.steps[].item',   // ← catches the GitHub editor bug
    'bring_section.tag',
    'bring_section.heading',
    'timeline_section.tag',
    'timeline_section.heading',
    'cta_strip.heading',
    'cta_strip.btn_url',
  ],
  'faq.md': [
    'header_h1',
  ],
  'precinct-locator.md': [
    'header_h1',
  ],
};

// ── Run ────────────────────────────────────────────────────────────────────

const contentDir = join(process.cwd(), 'content', 'pages');
const errors = [];
let checked = 0;

for (const [filename, requiredKeys] of Object.entries(RULES)) {
  const filePath = join(contentDir, filename);
  let raw;
  try {
    raw = readFileSync(filePath, 'utf8');
  } catch {
    errors.push(`  ✗ ${filename}: file not found at ${filePath}`);
    continue;
  }

  const frontmatter = parseFrontmatter(raw);
  if (!frontmatter) {
    errors.push(`  ✗ ${filename}: no frontmatter found (missing --- delimiters?)`);
    continue;
  }

  for (const key of requiredKeys) {
    checkKey(frontmatter, key, filename, errors);
  }
  checked++;
}

// ── Report ─────────────────────────────────────────────────────────────────

if (errors.length === 0) {
  console.log(`✅ Content validation passed (${checked} files checked — all required keys present).`);
  process.exit(0);
} else {
  console.error(`\n❌ Content validation FAILED — ${errors.length} issue(s) found:\n`);
  errors.forEach(e => console.error(e));
  console.error(`\nTip: The GitHub web editor sometimes strips YAML keys like "item:" from list items.`);
  console.error(`Always edit content files locally or double-check the YAML structure before pushing.\n`);
  process.exit(1);
}

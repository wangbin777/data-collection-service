import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';

const rootDir = process.cwd();
const includePrivate = process.argv.includes('--include-private');
const includeOverride = process.argv.includes('--include-override');
const issues = [];

function walk(dir, result = []) {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      walk(path, result);
    } else if (path.endsWith('.java')) {
      result.push(path);
    }
  }
  return result;
}

function removeLineComment(line) {
  const index = line.indexOf('//');
  return index >= 0 ? line.slice(0, index) : line;
}

function hasJavadocBefore(lines, declarationLine) {
  let index = declarationLine - 1;
  while (index >= 0 && lines[index].trim() === '') {
    index -= 1;
  }
  while (index >= 0 && lines[index].trim().startsWith('@')) {
    index -= 1;
    while (index >= 0 && lines[index].trim() === '') {
      index -= 1;
    }
  }
  if (index < 0 || !lines[index].includes('*/')) {
    return false;
  }
  while (index >= 0) {
    if (lines[index].includes('/**')) {
      return true;
    }
    if (lines[index].includes('/*') && !lines[index].includes('/**')) {
      return false;
    }
    index -= 1;
  }
  return false;
}

function hasOverrideBefore(lines, declarationLine) {
  let index = declarationLine - 1;
  while (index >= 0 && lines[index].trim() === '') {
    index -= 1;
  }
  while (index >= 0 && lines[index].trim().startsWith('@')) {
    if (lines[index].includes('@Override')) {
      return true;
    }
    index -= 1;
  }
  return false;
}

function isTrivialAccessor(name) {
  return /^(get|set|is)[A-Z0-9_]/.test(name);
}

function visibilityOf(line) {
  const match = line.trim().match(/^(public|protected|private)\b/);
  return match ? match[1] : 'package';
}

function detectDeclaration(lines, index, className) {
  const line = removeLineComment(lines[index]);
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith('*') || trimmed.startsWith('//') || trimmed.startsWith('/*')) {
    return null;
  }
  if (/\b(class|interface|enum|record)\s+[A-Za-z_][A-Za-z0-9_]*/.test(trimmed)
      && !trimmed.includes(' new ')) {
    const match = trimmed.match(/\b(class|interface|enum|record)\s+([A-Za-z_][A-Za-z0-9_]*)/);
    return { kind: match[1], name: match[2], visibility: visibilityOf(trimmed), trivial: false };
  }
  if (/^(if|for|while|switch|catch|return|throw|new|else|try|do)\b/.test(trimmed)) {
    return null;
  }
  if (!trimmed.includes('(') || trimmed.includes('->') || trimmed.includes('=')) {
    return null;
  }
  const constructorPattern = new RegExp(`^(public|protected|private)?\\s*${className}\\s*\\(`);
  const constructorMatch = trimmed.match(constructorPattern);
  if (constructorMatch) {
    return { kind: 'constructor', name: className, visibility: constructorMatch[1] ?? 'package', trivial: false };
  }
  const methodMatch = trimmed.match(/^(?:(public|protected|private)\s+)?(?:(?:static|final|abstract|synchronized|native|default|strictfp)\s+)*(?:[A-Za-z_][\w.$<>\[\],?\s]*\s+)+([A-Za-z_][A-Za-z0-9_]*)\s*\(/);
  if (!methodMatch) {
    return null;
  }
  const name = methodMatch[2];
  if (['if', 'for', 'while', 'switch', 'catch'].includes(name)) {
    return null;
  }
  return { kind: 'method', name, visibility: methodMatch[1] ?? 'package', trivial: isTrivialAccessor(name) };
}

function scanFile(file) {
  const content = readFileSync(file, 'utf8');
  const lines = content.split(/\r?\n/);
  let className = null;
  for (let index = 0; index < lines.length; index += 1) {
    const classMatch = removeLineComment(lines[index]).match(/\b(?:class|interface|enum|record)\s+([A-Za-z_][A-Za-z0-9_]*)/);
    if (classMatch) {
      className = classMatch[1];
    }
    const declaration = detectDeclaration(lines, index, className);
    if (!declaration) {
      continue;
    }
    if (declaration.kind === 'method') {
      if (!includePrivate && declaration.visibility === 'private') {
        continue;
      }
      if (!includeOverride && hasOverrideBefore(lines, index)) {
        continue;
      }
      if (declaration.trivial) {
        continue;
      }
    }
    if (declaration.kind === 'constructor' && !includePrivate && declaration.visibility === 'private') {
      continue;
    }
    if (!hasJavadocBefore(lines, index)) {
      issues.push({
        file: relative(rootDir, file).replaceAll('\\', '/'),
        line: index + 1,
        kind: declaration.kind,
        name: declaration.name,
        visibility: declaration.visibility,
      });
    }
  }
}

for (const file of walk(join(rootDir, 'src/main/java'))) {
  scanFile(file);
}

if (issues.length > 0) {
  console.error(JSON.stringify({ ok: false, count: issues.length, issues }, null, 2));
  process.exit(1);
}

console.log(JSON.stringify({ ok: true, count: 0 }, null, 2));

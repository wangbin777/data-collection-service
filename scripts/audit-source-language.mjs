import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import { TextDecoder } from 'node:util';

const rootDir = process.cwd();
const decoder = new TextDecoder('utf-8', { fatal: true });
const sourceRoots = [
  'src/main/java',
  'src/test/java',
  'src/main/resources',
  'scripts',
  '.',
];
const ignoredDirs = new Set(['.git', '.idea', '.hermes', '.m2-local', '.superpowers', 'target', 'tmp', 'node_modules', 'dist', 'release']);
const sourceExtensions = new Set([
  '.java',
  '.js',
  '.mjs',
  '.css',
  '.html',
  '.xml',
  '.yml',
  '.yaml',
  '.properties',
  '.md',
]);
const binaryExtensions = new Set([
  '.png',
  '.jpg',
  '.jpeg',
  '.gif',
  '.ico',
  '.svg',
]);
const skipLanguageFiles = new Set([
  'docs/modao-prototypes/task-result.raw.json',
]);
const codeCommentLinePattern = /^\s*(\/\/|\/\*\*?|\*)\s*(.*?)\s*(?:\*\/)?\s*$/;
const xmlCommentLinePattern = /^\s*(<!--|\*)\s*(.*?)\s*(?:-->)?\s*$/;
const configCommentLinePattern = /^\s*#\s*(.*?)\s*$/;
const logPattern = /\blog\.(trace|debug|info|warn|error)\(\s*"((?:\\"|[^"])*)"/g;
const cjkPattern = /[\u3400-\u9fff]/;
const englishPattern = /[A-Za-z]/;
const mixedLanguageResiduePattern = /\b(due to|injected|keep periodic|time slice execute|rate limit|dropped|Send|post-process|fallback|falling back|Cleanup|Unknown|Retry|Ignore|Release|stale|async|reload|temporary|allowed|is not|supported|process|before|after|single|random|execute|logical devices|spontaneous|subscribe|subscription|unsubscribe|loaded|plans rebuilt|trigger interrogation|transport|rawValue=|deviceId=|pointId=|pointCode=|pointName=|gatewayDeviceId=|messageId=|responseCode=|batchSize=|count=|size=|bytes=|topic=|qos=|key=|point=|device=|stage=|error=|success=|reason=|attempt=|generation=)\b/i;

const issues = [];

function extensionOf(path) {
  const index = path.lastIndexOf('.');
  return index >= 0 ? path.slice(index).toLowerCase() : '';
}

function walk(dir, result) {
  for (const name of readdirSync(dir)) {
    if (ignoredDirs.has(name)) {
      continue;
    }
    const path = join(dir, name);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      walk(path, result);
      continue;
    }
    result.push(path);
  }
}

function allFiles() {
  const files = [];
  for (const sourceRoot of sourceRoots) {
    const path = join(rootDir, sourceRoot);
    if (statSync(path, { throwIfNoEntry: false })?.isDirectory()) {
      walk(path, files);
    } else if (statSync(path, { throwIfNoEntry: false })?.isFile()) {
      files.push(path);
    }
  }
  return [...new Set(files)];
}

function addIssue(file, line, rule, message) {
  issues.push({
    file: relative(rootDir, file).replaceAll('\\', '/'),
    line,
    rule,
    message,
  });
}

function readUtf8(file) {
  const bytes = readFileSync(file);
  try {
    return decoder.decode(bytes);
  } catch (error) {
    addIssue(file, 1, 'utf8', `文件不能按 UTF-8 解码：${error.message}`);
    return null;
  }
}

function shouldScanLanguage(file) {
  const rel = relative(rootDir, file).replaceAll('\\', '/');
  if (skipLanguageFiles.has(rel)
    || rel.startsWith('docs/')
    || rel.startsWith('collector-desktop/docs/')
    || rel.startsWith('collector-boot/src/main/resources/static/desktop/')) {
    return false;
  }
  return true;
}

function containsEnglishWithoutChinese(text) {
  return englishPattern.test(text) && !cjkPattern.test(text);
}

function scanCommentLanguage(file, content) {
  if (!shouldScanLanguage(file)) {
    return;
  }
  const ext = extensionOf(file);
  const commentLinePattern = ext === '.xml' || file === join(rootDir, 'pom.xml')
    ? xmlCommentLinePattern
    : (ext === '.yml' || ext === '.yaml' || ext === '.properties' ? configCommentLinePattern : codeCommentLinePattern);
  const lines = content.split(/\r?\n/);
  lines.forEach((line, index) => {
    const match = line.match(commentLinePattern);
    if (!match) {
      return;
    }
    const text = (match[2] ?? match[1] ?? '')
      .replace(/\{@link\s+[^}]+}/g, '')
      .replace(/\{@code\s+[^}]+}/g, '')
      .trim();
    if (!text || text.startsWith('http://') || text.startsWith('https://')) {
      return;
    }
    if (containsEnglishWithoutChinese(text)) {
      addIssue(file, index + 1, 'comment-language', `注释需要使用中文：${text}`);
      return;
    }
    if (mixedLanguageResiduePattern.test(text)) {
      addIssue(file, index + 1, 'comment-language', `注释存在未翻译英文片段：${text}`);
    }
  });
}

function scanLogLanguage(file, content) {
  if (!relative(rootDir, file).replaceAll('\\', '/').startsWith('src/main/java/')) {
    return;
  }
  const lines = content.split(/\r?\n/);
  lines.forEach((line, index) => {
    let match;
    while ((match = logPattern.exec(line)) !== null) {
      const literal = match[2].replace(/\\"/g, '"');
      if (containsEnglishWithoutChinese(literal)) {
        addIssue(file, index + 1, 'log-language', `日志需要使用中文：${literal}`);
        continue;
      }
      if (mixedLanguageResiduePattern.test(literal)) {
        addIssue(file, index + 1, 'log-language', `日志存在未翻译英文片段：${literal}`);
      }
    }
    if (/\bSystem\.(out|err)\./.test(line)) {
      addIssue(file, index + 1, 'system-output', '生产代码禁止直接使用 System.out/System.err 输出。');
    }
  });
}

function scanFile(file) {
  const ext = extensionOf(file);
  if (binaryExtensions.has(ext)) {
    return;
  }
  if (!sourceExtensions.has(ext) && file !== join(rootDir, 'pom.xml')) {
    return;
  }
  const content = readUtf8(file);
  if (content === null) {
    return;
  }
  scanCommentLanguage(file, content);
  scanLogLanguage(file, content);
}

for (const file of allFiles()) {
  scanFile(file);
}

if (issues.length > 0) {
  console.error(JSON.stringify({
    ok: false,
    count: issues.length,
    issues,
  }, null, 2));
  process.exit(1);
}

console.log(JSON.stringify({
  ok: true,
  count: 0,
}, null, 2));


// Syntax colors for code generated in the browser, matching the site's
// shiki themes (github-light-high-contrast / ayu-dark) so it sits
// alongside the markdown code blocks. Emits the same --shiki-light /
// --shiki-dark spans VitePress renders, one .line span per line.

type Color = [light: string, dark: string];

const KEYWORD: Color = ['#A0111F', '#FF8F40'];
const OPERATOR: Color = ['#A0111F', '#F29668'];
const STRING: Color = ['#032563', '#AAD94C'];
const NUMBER: Color = ['#023B95', '#D2A6FF'];
const COMMENT: Color = ['#66707B', '#5A6673'];
const METHOD: Color = ['#622CBC', '#FFB454'];
const TYPE: Color = ['#702C00', '#59C2FF'];

const KEYWORDS = new Set(['using', 'var', 'await', 'new', 'return', 'true', 'false', 'null']);

// comment | string | number | operator | identifier | anything else
const CSHARP = /(\/\/.*|\/\*.*?\*\/)|(\$?"(?:[^"\\]|\\.)*")|(\b\d+\b)|(=>|=)|([A-Za-z_]\w*)|(\s+|.)/g;

function escape(text: string) {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function span(text: string, color?: Color) {
  return color
    ? `<span style="--shiki-light:${color[0]};--shiki-dark:${color[1]};">${escape(text)}</span>`
    : escape(text);
}

function csharpLine(line: string) {
  // namespaces in a using directive read as types throughout
  const isUsing = line.startsWith('using ');
  let out = '';
  for (const m of line.matchAll(CSHARP)) {
    const [text, comment, str, num, op, ident] = m;
    if (comment) out += span(text, COMMENT);
    else if (str) out += span(text, STRING);
    else if (num) out += span(text, NUMBER);
    else if (op) out += span(text, OPERATOR);
    else if (ident) {
      const rest = line.slice(m.index! + text.length);
      if (KEYWORDS.has(text)) out += span(text, KEYWORD);
      else if (/^(<[\w<>, ]*>)?\(/.test(rest)) out += span(text, METHOD);
      else if (/^[A-Z]/.test(text) && (isUsing || line[m.index! - 1] !== '.')) out += span(text, TYPE);
      else out += span(text);
    } else out += span(text);
  }
  return out;
}

// a command, then its arguments
function bashLine(line: string) {
  const [, cmd, args] = /^(\S*)(.*)$/.exec(line)!;
  return span(cmd, TYPE) + (args ? span(args, STRING) : '');
}

export function highlight(code: string, lang: 'csharp' | 'bash') {
  const render = lang === 'csharp' ? csharpLine : bashLine;
  return code
    .split('\n')
    .map(line => `<span class="line">${render(line)}</span>`)
    .join('\n');
}

#!/usr/bin/env node
import { execFileSync } from 'node:child_process'
import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const repoRoot = resolve(__dirname, '../..')
const outFile = resolve(repoRoot, 'docs/03-interfaces/rpc-api.md')

const TYPE_MAP = {
  TYPE_DOUBLE: 'double',
  TYPE_FLOAT: 'float',
  TYPE_INT64: 'int64',
  TYPE_UINT64: 'uint64',
  TYPE_INT32: 'int32',
  TYPE_FIXED64: 'fixed64',
  TYPE_FIXED32: 'fixed32',
  TYPE_BOOL: 'bool',
  TYPE_STRING: 'string',
  TYPE_BYTES: 'bytes',
  TYPE_UINT32: 'uint32',
  TYPE_SFIXED32: 'sfixed32',
  TYPE_SFIXED64: 'sfixed64',
  TYPE_SINT32: 'sint32',
  TYPE_SINT64: 'sint64',
}

/**
 * 转义 Markdown 普通文本中的 Vue SFC 敏感字符 (<, >, {, })，保留反引号包裹的行内代码不变。
 */
function escapeVueSfc(text) {
  if (!text) return ''
  return text
    .split(/(`[^`\n]+`)/g)
    .map((part, idx) => {
      if (idx % 2 === 1) {
        // 行内代码段仅防御性阻断 Vue 双花括号插值，不改变其它字符
        return part.replace(/\{\{/g, '&#123;&#123;').replace(/\}\}/g, '&#125;&#125;')
      }
      return part
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\{/g, '&#123;')
        .replace(/\}/g, '&#125;')
    })
    .join('')
}

/**
 * 清洗并格式化用于 Markdown 表格单元格的文本。
 */
function formatCellText(text) {
  if (!text) return '—'
  const normalized = text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .join('<br>')
  return escapeVueSfc(normalized).replace(/\|/g, '\\|')
}

/**
 * 清洗并格式化用于独立段落的多行注释。
 */
function formatParagraph(text) {
  if (!text) return ''
  return text
    .split('\n')
    .map((line) => escapeVueSfc(line.trim()))
    .filter(Boolean)
    .join('\n\n')
}

/**
 * 生成大小写一致的 VitePress 标题锚点。
 */
function toAnchor(name) {
  return name.toLowerCase().replace(/[^a-z0-9_-]+/g, '-')
}

function shortTypeName(fullTypeName) {
  if (!fullTypeName) return ''
  return fullTypeName.replace(/^\.storage\.v1\./, '').replace(/^\./, '')
}

function buildCommentMap(file) {
  const map = new Map()
  for (const loc of file.sourceCodeInfo?.location ?? []) {
    const key = (loc.path ?? []).join(',')
    const parts = [loc.leadingComments, loc.trailingComments]
      .filter(Boolean)
      .map((s) => s.trim())
      .filter(Boolean)
    if (parts.length > 0) {
      map.set(key, parts.join('\n'))
    }
  }
  return map
}

function formatScalarRule(ruleName, ruleObj) {
  if (!ruleObj || typeof ruleObj !== 'object') return `\`${ruleName}\``
  const parts = Object.entries(ruleObj).map(([k, v]) => {
    const valStr = Array.isArray(v) || typeof v === 'object' ? JSON.stringify(v) : String(v)
    return `\`${k}=${valStr}\``
  })
  return `\`${ruleName}\`: ${parts.join(', ')}`
}

function formatCelList(celList) {
  return celList.map((c) => {
    const expr = String(c.expression ?? '').replace(/`/g, "'")
    const msg = c.message ? `（${escapeVueSfc(c.message)}）` : ''
    return `CEL \`${c.id}\`: \`${expr}\`${msg}`
  })
}

function formatFieldValidateRule(rule) {
  if (!rule || typeof rule !== 'object') return []
  const items = []
  if (rule.required) {
    items.push('`required`')
  }
  for (const [key, val] of Object.entries(rule)) {
    if (key === 'required') continue
    if (key === 'cel' && Array.isArray(val)) {
      items.push(...formatCelList(val))
    } else if (key === 'repeated' && val && typeof val === 'object') {
      const sub = []
      if (val.minItems !== undefined) sub.push(`\`min_items=${val.minItems}\``)
      if (val.maxItems !== undefined) sub.push(`\`max_items=${val.maxItems}\``)
      if (val.unique) sub.push('`unique=true`')
      if (val.items) {
        const itemRules = formatFieldValidateRule(val.items)
        if (itemRules.length > 0) sub.push(`元素: ${itemRules.join('; ')}`)
      }
      items.push(`\`repeated\`: ${sub.join(', ')}`)
    } else if (key === 'map' && val && typeof val === 'object') {
      const sub = []
      if (val.minPairs !== undefined) sub.push(`\`min_pairs=${val.minPairs}\``)
      if (val.maxPairs !== undefined) sub.push(`\`max_pairs=${val.maxPairs}\``)
      if (val.keys) {
        const keyRules = formatFieldValidateRule(val.keys)
        if (keyRules.length > 0) sub.push(`Key: ${keyRules.join('; ')}`)
      }
      if (val.values) {
        const valRules = formatFieldValidateRule(val.values)
        if (valRules.length > 0) sub.push(`Value: ${valRules.join('; ')}`)
      }
      items.push(`\`map\`: ${sub.join(', ')}`)
    } else {
      items.push(formatScalarRule(key, val))
    }
  }
  return items
}

function resolveFieldType(field, nestedMapEntries, localNames) {
  if (field.type === 'TYPE_MESSAGE' && field.typeName) {
    const clean = shortTypeName(field.typeName)
    const mapEntry = nestedMapEntries.get(clean)
    if (mapEntry) {
      const keyType = resolveFieldType(mapEntry.keyField, nestedMapEntries, localNames)
      const valType = resolveFieldType(mapEntry.valueField, nestedMapEntries, localNames)
      return `map&lt;${keyType}, ${valType}&gt;`
    }
    if (localNames.has(clean)) {
      return `[${clean}](#${toAnchor(clean)})`
    }
    return `\`${clean}\``
  }
  if (field.type === 'TYPE_ENUM' && field.typeName) {
    const clean = shortTypeName(field.typeName)
    if (localNames.has(clean)) {
      return `[${clean}](#${toAnchor(clean)})`
    }
    return `\`${clean}\``
  }
  return `\`${TYPE_MAP[field.type] ?? field.type}\``
}

function resolveFieldLabel(field, msg, isMapField) {
  if (isMapField) return 'map'
  if (field.proto3Optional) return 'optional'
  if (field.label === 'LABEL_REPEATED') return 'repeated'
  if (field.oneofIndex !== undefined && !field.proto3Optional) {
    const oneofName = msg.oneofDecl?.[field.oneofIndex]?.name
    return oneofName ? `oneof \`${oneofName}\`` : 'oneof'
  }
  return 'singular'
}

function formatStreaming(method) {
  if (method.clientStreaming && method.serverStreaming) return '双向流式'
  if (method.clientStreaming) return '客户端流式'
  if (method.serverStreaming) return '服务端流式'
  return '一元调用 (Unary)'
}

function main() {
  const raw = execFileSync('buf', ['build', '--exclude-imports', '-o', '-#format=json'], {
    cwd: repoRoot,
    encoding: 'utf8',
    maxBuffer: 50 * 1024 * 1024,
  })
  const descriptorSet = JSON.parse(raw)
  const file =
    descriptorSet.file?.find((f) => f.name === 'storage/v1/storage.proto') ?? descriptorSet.file?.[0]
  if (!file) {
    throw new Error('未在 buf build 输出中找到 storage/v1/storage.proto 描述符')
  }

  const comments = buildCommentMap(file)
  const localNames = new Set([
    ...(file.enumType ?? []).map((e) => e.name),
    ...(file.messageType ?? []).map((m) => m.name),
  ])

  const nestedMapEntries = new Map()
  for (const msg of file.messageType ?? []) {
    for (const nested of msg.nestedType ?? []) {
      if (nested.options?.mapEntry) {
        const keyField = nested.field?.find((f) => f.number === 1)
        const valueField = nested.field?.find((f) => f.number === 2)
        if (keyField && valueField) {
          nestedMapEntries.set(`${msg.name}.${nested.name}`, { keyField, valueField })
        }
      }
    }
  }

  const lines = []
  lines.push('<!-- 本文档由 `task docs:gen`（`docs/scripts/gen-rpc-doc.mjs`）从 `proto/storage/v1/storage.proto` 自动生成，请勿手动修改。 -->')
  lines.push('')
  lines.push('# RPC 接口契约')
  lines.push('')
  lines.push(
    `> 本文档基于 \`${file.name}\`（包名 \`${file.package}\`）通过 \`buf build --exclude-imports -o -#format=json\` 自动提取生成，修改契约后请执行 \`task docs:gen\` 同步更新。`,
  )
  lines.push('')

  // 1. Service & RPCs
  for (let sIdx = 0; sIdx < (file.service ?? []).length; sIdx++) {
    const svc = file.service[sIdx]
    const svcComment = comments.get(`6,${sIdx}`)
    const methods = svc.method ?? []

    lines.push(`## 服务定义：${svc.name}`)
    lines.push('')
    if (svcComment) {
      lines.push(formatParagraph(svcComment))
      lines.push('')
    }
    lines.push(`共包含 **${methods.length}** 个 RPC 方法：`)
    lines.push('')
    lines.push('| # | RPC 方法 | 请求类型 | 响应类型 | 调用模式 |')
    lines.push('| --- | --- | --- | --- | --- |')
    methods.forEach((m, mIdx) => {
      const req = shortTypeName(m.inputType)
      const res = shortTypeName(m.outputType)
      const reqLink = localNames.has(req) ? `[${req}](#${toAnchor(req)})` : `\`${req}\``
      const resLink = localNames.has(res) ? `[${res}](#${toAnchor(res)})` : `\`${res}\``
      lines.push(
        `| ${mIdx + 1} | [${m.name}](#${toAnchor(m.name)}) | ${reqLink} | ${resLink} | ${formatStreaming(m)} |`,
      )
    })
    lines.push('')

    lines.push('### RPC 方法详述')
    lines.push('')
    methods.forEach((m, mIdx) => {
      const req = shortTypeName(m.inputType)
      const res = shortTypeName(m.outputType)
      const reqLink = localNames.has(req) ? `[${req}](#${toAnchor(req)})` : `\`${req}\``
      const resLink = localNames.has(res) ? `[${res}](#${toAnchor(res)})` : `\`${res}\``
      const mComment = comments.get(`6,${sIdx},2,${mIdx}`)

      lines.push(`#### ${m.name}`)
      lines.push('')
      lines.push(
        `- **请求**: ${reqLink}　|　**响应**: ${resLink}　|　**模式**: ${formatStreaming(m)}`,
      )
      lines.push('')
      if (mComment) {
        lines.push(formatParagraph(mComment))
        lines.push('')
      }
    })
  }

  // 2. Enums
  const enums = file.enumType ?? []
  if (enums.length > 0) {
    lines.push('## 枚举类型定义')
    lines.push('')
    enums.forEach((e, eIdx) => {
      const eComment = comments.get(`5,${eIdx}`)
      lines.push(`### ${e.name}`)
      lines.push('')
      if (eComment) {
        lines.push(formatParagraph(eComment))
        lines.push('')
      }
      lines.push('| 枚举值 | 数值 | 说明 |')
      lines.push('| --- | --- | --- |')
      ;(e.value ?? []).forEach((v, vIdx) => {
        const vComment = comments.get(`5,${eIdx},2,${vIdx}`)
        lines.push(`| \`${v.name}\` | \`${v.number}\` | ${formatCellText(vComment)} |`)
      })
      lines.push('')
    })
  }

  // 3. Messages
  const messages = file.messageType ?? []
  if (messages.length > 0) {
    lines.push('## 消息结构定义')
    lines.push('')
    messages.forEach((msg, msgIdx) => {
      const msgComment = comments.get(`4,${msgIdx}`)
      lines.push(`### ${msg.name}`)
      lines.push('')
      if (msgComment) {
        lines.push(formatParagraph(msgComment))
        lines.push('')
      }

      const msgRule = msg.options?.['[buf.validate.message]']
      if (msgRule?.cel && Array.isArray(msgRule.cel)) {
        lines.push('**消息级校验规则（protovalidate）：**')
        lines.push('')
        for (const item of formatCelList(msgRule.cel)) {
          lines.push(`- ${item}`)
        }
        lines.push('')
      }

      const fields = msg.field ?? []
      if (fields.length === 0) {
        lines.push('*无字段（空消息）。*')
        lines.push('')
        return
      }

      lines.push('| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |')
      lines.push('| --- | --- | --- | --- | --- | --- |')
      fields.forEach((f, fIdx) => {
        const fComment = comments.get(`4,${msgIdx},2,${fIdx}`)
        const cleanType = f.typeName ? shortTypeName(f.typeName) : ''
        const isMapField = f.type === 'TYPE_MESSAGE' && nestedMapEntries.has(cleanType)
        const typeStr = resolveFieldType(f, nestedMapEntries, localNames)
        const labelStr = resolveFieldLabel(f, msg, isMapField)
        const ruleObj = f.options?.['[buf.validate.field]']
        const rules = formatFieldValidateRule(ruleObj)
        const ruleStr =
          rules.length > 0 ? rules.map((r) => r.replace(/\|/g, '\\|')).join('<br>') : '—'

        lines.push(
          `| ${f.number} | \`${f.name}\` | ${typeStr} | ${labelStr} | ${ruleStr} | ${formatCellText(fComment)} |`,
        )
      })
      lines.push('')
    })
  }

  mkdirSync(dirname(outFile), { recursive: true })
  writeFileSync(outFile, `${lines.join('\n')}\n`, 'utf8')
  console.log(`Generated ${outFile} (${(file.service?.[0]?.method ?? []).length} RPCs, ${messages.length} messages, ${enums.length} enums)`)
}

main()

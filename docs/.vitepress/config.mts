import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

export default withMermaid(
  defineConfig({
    lang: 'zh-CN',
    title: 'Bowl Developer Book',
    description: '分布式虚拟磁盘系统架构、功能规格与二次开发手册',
    ignoreDeadLinks: [/^https?:\/\/(localhost|127\.0\.0\.1)/],
    vite: {
      optimizeDeps: {
        include: ['mermaid', 'dayjs'],
      },
    },
    themeConfig: {
      search: {
        provider: 'local',
      },
      nav: [
        { text: '首页', link: '/' },
        {
          text: '全局认知',
          items: [
            { text: '能力全景', link: '/01-overview/capabilities' },
            { text: '系统架构', link: '/01-overview/architecture' },
            { text: '快速开始', link: '/01-overview/quickstart' },
          ],
        },
        {
          text: '功能规格',
          items: [
            { text: '磁盘生命周期', link: '/02-features/disk-lifecycle' },
            { text: '派生、差异与合并', link: '/02-features/fork-diff-merge' },
            { text: '挂载租约与写栅栏', link: '/02-features/mount-and-fencing' },
            { text: 'POSIX 语义与扩展属性', link: '/02-features/posix-semantics' },
          ],
        },
        {
          text: '接口契约',
          items: [
            { text: 'CLI 命令参考', link: '/03-interfaces/cli-reference' },
            { text: 'RPC 接口契约', link: '/03-interfaces/rpc-api' },
            { text: '可观测性与运维指标', link: '/03-interfaces/observability' },
          ],
        },
        {
          text: '内部实现与扩展',
          items: [
            { text: 'Storage 控制面实现', link: '/04-internals/storage-control-plane' },
            { text: 'Storage 存储引擎', link: '/04-internals/storage-store-engine' },
            { text: 'Client 与 FUSE 流水线', link: '/04-internals/client-fuse-pipeline' },
            { text: '二次开发与扩展指南', link: '/04-internals/extending-bowl' },
          ],
        },
      ],
      sidebar: [
        {
          text: '第一卷：全局认知',
          items: [
            { text: '能力全景', link: '/01-overview/capabilities' },
            { text: '系统架构', link: '/01-overview/architecture' },
            { text: '快速开始', link: '/01-overview/quickstart' },
          ],
        },
        {
          text: '第二卷：功能规格',
          items: [
            { text: '磁盘生命周期', link: '/02-features/disk-lifecycle' },
            { text: '派生、差异与三路合并', link: '/02-features/fork-diff-merge' },
            { text: '挂载租约与写栅栏', link: '/02-features/mount-and-fencing' },
            { text: 'POSIX 语义与扩展属性', link: '/02-features/posix-semantics' },
          ],
        },
        {
          text: '第三卷：接口契约',
          items: [
            { text: 'CLI 命令参考', link: '/03-interfaces/cli-reference' },
            { text: 'RPC 接口契约', link: '/03-interfaces/rpc-api' },
            { text: '可观测性与运维指标', link: '/03-interfaces/observability' },
          ],
        },
        {
          text: '第四卷：内部实现与扩展',
          items: [
            { text: 'Storage 控制面实现', link: '/04-internals/storage-control-plane' },
            { text: 'Storage 存储引擎', link: '/04-internals/storage-store-engine' },
            { text: 'Client 与 FUSE 流水线', link: '/04-internals/client-fuse-pipeline' },
            { text: '二次开发与扩展指南', link: '/04-internals/extending-bowl' },
          ],
        },
      ],
      outline: {
        label: '本页目录',
        level: [2, 3],
      },
      docFooter: {
        prev: '上一章',
        next: '下一章',
      },
    },
  }),
)

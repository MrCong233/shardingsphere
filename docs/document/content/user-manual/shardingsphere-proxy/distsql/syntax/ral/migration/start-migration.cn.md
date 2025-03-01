+++
title = "START MIGRATION"
weight = 16
+++

### 描述

`START MIGRATION` 语法用于开始指定的数据迁移作业。

### 语法

{{< tabs >}}
{{% tab name="语法" %}}
```sql
StartMigration ::=
  'START' 'MIGRATION'  migrationJobId 

migrationJobId ::=
  string
```
{{% /tab %}}
{{% tab name="铁路图" %}}
<iframe frameborder="0" name="diagram" id="diagram" width="100%" height="100%"></iframe>
{{% /tab %}}
{{< /tabs >}}

### 补充说明

- `migrationJobId` 需要通过 [SHOW MIGRATION LIST](/cn/user-manual/shardingsphere-proxy/distsql/syntax/ral/migration/show-migration-list/) 语法查询获得

### 示例

- 停止指定的数据迁移作业

```sql
START MIGRATION 'j010180026753ef0e25d3932d94d1673ba551';
```

### 保留字

`START`、`MIGRATION`

### 相关链接

- [保留字](/cn/user-manual/shardingsphere-proxy/distsql/syntax/reserved-word/)
- [SHOW MIGRATION LIST](/cn/user-manual/shardingsphere-proxy/distsql/syntax/ral/migration/show-migration-list/)
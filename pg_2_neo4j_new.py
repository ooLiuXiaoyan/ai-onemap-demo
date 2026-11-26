import datetime
import psycopg2
from neo4j import GraphDatabase
import logging
from typing import List, Dict, Any

# 数据，创建关系图谱

# === 配置 ===
PG_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'xxxx',
    'user': 'test',
    'password': 'test'
}

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "testtest"

# === 表配置（核心！可扩展）===
TABLES = [
    {"name": "jggcgxb", "label": "JGGCGXB", "id_fields": ["GD_GUID", "XZ_GUID", "YD_XKZ_GUID", "GC_XKZ_GUID"], "id_uniq": "GX_GUID"},
    {"name": "gd_gdxm", "label": "GD_GDXM", "id_fields": ["GD_GUID"], "id_uniq":"GD_GUID"},
    {"name": "st_gddk", "label": "ST_GDDK", "id_fields": ["XM_GUID","DK_GUID"], "id_uniq":"DK_GUID"},
    
    {"name": "ysxz_jbxx", "label": "YSXZ_JBXX", "id_fields": ["XM_GUID"], "id_uniq": "XM_GUID"},
    {"name": "st_ysdk", "label": "ST_YSDK", "id_fields": ["XM_GUID", "DK_GUID"], "id_uniq": "DK_GUID"},

    {"name": "gh_jsydghxk", "label": "GH_JSYDGHXK", "id_fields": ["XKZ_GUID"], "id_uniq": "XKZ_GUID"},
    {"name": "gh_jsydghxkz", "label": "GH_JSYDGHXKZ", "id_fields": ["XKZ_GUID"],"id_uniq": "XKZ_GUID"},
    {"name": "st_ghxkdk", "label": "ST_GHXKDK", "id_fields": ["XM_GUID", "DK_GUID"], "id_uniq": "DK_GUID"},

    {"name": "gc_jsgcghxk", "label": "GC_JSGCGHXK", "id_fields": ["XKZ_GUID"], "id_uniq": "XKZ_GUID"},
    {"name": "gc_ghyx", "label": "GC_GHYX", "id_fields": ["GHYX_GUID"], "id_uniq": "GHYX_GUID"},
    {"name": "gc_gcsjfa", "label": "GC_GCSJFA", "id_fields": ["XM_GUID"], "id_uniq": "XM_GUID"},
    {"name": "gc_fxgczb", "label": "GC_FXGCZB", "id_fields": ["XKZ_GUID"], "id_uniq": "XKZ_GUID"},
    {"name": "st_gcxkdk", "label": "ST_GCXKDK", "id_fields": ["XM_GUID"], "id_uniq": ""},

    {"name": "jg_ghhy", "label": "JG_GHHY", "id_fields": ["JG_GUID"], "id_uniq": "JG_GUID"},
    {"name": "jg_ghhyjzmx", "label": "JG_GHHYJZMX", "id_fields": ["JG_GUID", "DK_GUID"], "id_uniq":"ZBMX_GUID"},
    {"name": "jg_ghhyht_gx", "label": "JG_GHHYHT_GX", "id_fields": ["JG_GUID"], "id_uniq":""},
    {"name": "jg_ghhykzzb", "label": "JG_GHHYKZZB", "id_fields": ["JG_GUID"], "id_uniq":"JG_GUID"},
    {"name": "jg_fxgczb", "label": "JG_FXGCZB", "id_fields": ["GHHY_GUID"], "id_uniq":"GHHY_GUID"},
    {"name": "st_jghsdk", "label": "ST_JGHSDK", "id_fields": ["XM_GUID"]},
    {"name": "st_jgysdk", "label": "ST_JGYSDK", "id_fields": ["XM_GUID"]},

    # 二阶段 农转用和农转用报批节点
    {"name": "nzy_bpxm", "label": "NZY_BPXM", "id_fields": ["XM_GUID"], "id_uniq": "XM_GUID"},
    {"name": "nzy_bpxm_pzmj", "label": "NZY_BPXM_PZMJ", "id_fields": ["XM_GUID"], "id_uniq": "xm_guid"},
    {"name": "nzy_nzyxmbp", "label": "NZY_NZYXMBP", "id_fields": ["BPXM_GUID", "ZYXM_GUID", "GX_GUID"], "id_uniq": "GX_GUID"},
    {"name": "nzy_nzyxm", "label": "NZY_NZYXM", "id_fields": ["XM_GUID"], "id_uniq": "XM_GUID"},
    {"name": "st_nzydk", "label": "ST_NZYDK", "id_fields": ["XM_GUID", "DK_GUID"], "id_uniq": "DK_GUID"},

    # 三阶段 
    {"name": "cb_cbxm", "label": "CB_CBXM", "id_fields": ["CBXM_GUID"], "id_uniq": "CBXM_GUID"},
    {"name": "cb_cbdk", "label": "CB_CBDK", "id_fields": ["CBDK_GUID"], "id_uniq": "CBDK_GUID"},
    {"name": "st_cbdk", "label": "ST_CBDK", "id_fields": ["DK_XH", "DK_BH", "DK_GUID"], "id_uniq":"DK_XH"},

    # 三调，总规，控规
    {"name": "st_dltb", "label": "ST_DLTB", "id_fields": ["OBJECTID"], "id_uniq":"OBJECTID"},
    {"name": "st_ghdl", "label": "ST_GHDL", "id_fields": ["OBJECTID"], "id_uniq":"OBJECTID"},
    {"name": "st_kgdk", "label": "ST_KGDK", "id_fields": ["OBJECTID"], "id_uniq":"OBJECTID"},
]

# === 关系规则（字段值匹配）===
RELATIONSHIPS = [
    # (源表, 源字段, 目标表, 目标字段, 关系类型)
    ("jggcgxb", "GD_GUID", "gd_gdxm", "GD_GUID", "HAS_PROJECT"),
    ("st_gddk", "XM_GUID", "gd_gdxm", "GD_GUID", "BELONGS_TO_PROJECT"),

    ("jggcgxb", "XZ_GUID", "ysxz_jbxx", "XM_GUID", "HAS_PRE_APPROVAL"),
    ("st_ysdk", "XM_GUID", "ysxz_jbxx", "XM_GUID", "BELONGS_TO_PRE_APPROVAL"),

    ("jggcgxb", "YD_XKZ_GUID", "gh_jsydghxk", "XKZ_GUID", "HAS_LAND_USE_PLANNING_PERMIT"),
    ("gh_jsydghxkz", "XKZ_GUID", "gh_jsydghxk", "XKZ_GUID", "BELOGS_TO_LAND_USE_PLANNING_PERMIT"),
    ("st_ghxkdk", "XM_GUID", "gh_jsydghxk", "XKZ_GUID", "BELOGS_TO_LAND_USE_PLANNING_PERMIT"),

    ("jggcgxb", "GC_XKZ_GUID", "gc_jsgcghxk", "XKZ_GUID", "HAS_CONSTRUCTION_PROJECT_PLANNING_PERMIT"),
    ("gc_ghyx", "GHYX_GUID", "gc_jsgcghxk", "XKZ_GUID", "BELOGS_TO_CONSTRUCTION_PROJECT_PLANNING_PERMIT"),
    ("gc_gcsjfa", "XM_GUID", "gc_jsgcghxk", "XKZ_GUID", "BELOGS_TO_CONSTRUCTION_PROJECT_PLANNING_PERMIT"),
    ("gc_fxgczb", "XKZ_GUID", "gc_jsgcghxk", "XKZ_GUID", "BELOGS_TO_CONSTRUCTION_PROJECT_PLANNING_PERMIT"),
    ("st_gcxkdk", "XM_GUID", "gc_jsgcghxk", "XKZ_GUID", "BELOGS_TO_CONSTRUCTION_PROJECT_PLANNING_PERMIT"),

    ("jggcgxb", "JG_GUID", "jg_ghhy", "JG_GUID", "HAS_POST_CONSTRUCTION_PLANNING_VERIFICATION"),
    ("jg_ghhyjzmx", "JG_GUID", "jg_ghhy", "JG_GUID", "BELOGS_TO_POST_CONSTRUCTION_PLANNING_VERIFICATION"),
    ("jg_ghhyht_gx", "JG_GUID", "jg_ghhy", "JG_GUID", "BELOGS_TO_POST_CONSTRUCTION_PLANNING_VERIFICATION"),
    ("jg_ghhykzzb", "JG_GUID", "jg_ghhy", "JG_GUID", "BELOGS_TO_POST_CONSTRUCTION_PLANNING_VERIFICATION"),
    ("jg_fxgczb", "GHHY_GUID", "jg_ghhy", "JG_GUID", "BELOGS_TO_POST_CONSTRUCTION_PLANNING_VERIFICATION"),
    ("st_jghsdk", "XM_GUID", "jg_ghhy", "JG_GUID", "BELOGS_TO_POST_CONSTRUCTION_PLANNING_VERIFICATION"),
    ("st_jgysdk", "XM_GUID", "jg_ghhy", "JG_GUID", "BELOGS_TO_POST_CONSTRUCTION_PLANNING_VERIFICATION"),
     
    ("st_nzydk", "XM_GUID", "nzy_nzyxm", "XM_GUID", "BELONGS_TO_PROJECT"),
    ("nzy_nzyxmbp", "ZYXM_GUID", "nzy_nzyxm", "XM_GUID", "HAS_PROJECT"),
    ("nzy_bpxm", "XM_GUID", "nzy_nzyxmbp", "PBXM_GUID", "BELOGS_TO__PROJECT"),
    ("nzy_bpxm_pzmj", "XM_GUID", "nzy_bpxm", "XM_GUID", "BELOGS_TO__PROJECT"),

    ("st_cbdk", "DK_BH", "cb_cbdk", "CBDK_GUID", "BELONGS_TO_PROJECT"),
    ("cb_cbdk", "CBDK_GUID", "cb_cbxm", "CBXM_GUID", "BELONGS_TO_PROJECT"),
]

# === 空间叠加关系配置 ===
SPATIAL_RELATIONSHIPS = [
    # (表A, geom_field_a, 表B, geom_field_b, rel_type)
    ("st_nzydk", "geometry", "st_gddk", "geometry", "OVERLAPS_WITH"),
    ("st_cbdk", "geometry", "st_gddk", "geometry", "OVERLAPS_WITH"),
    ("st_dltb", "geometry", "st_gddk", "geometry", "OVERLAPS_WITH"),
    ("st_ghdl", "geometry", "st_gddk", "geometry", "OVERLAPS_WITH"),
    ("st_kgdk", "geometry", "st_gddk", "geometry", "OVERLAPS_WITH"),
    # 可继续添加，支持不同字段名
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === 1. 从 PG 读取表数据 ===
def fetch_table_data(pg_config: dict, table_name: str) -> List[Dict[str, Any]]:
    """读取整张表的数据（返回字典列表）"""
    conn = psycopg2.connect(**pg_config)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name};")
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def sanitize_value(value):
    """将 datetime 对象转为无时区格式，其他类型不变"""
    if isinstance(value, datetime.datetime):
        # 移除 tzinfo，避免 Neo4j 报错
        return value.replace(tzinfo=None)
    return value


# === 2. 导入节点到 Neo4j ===
def create_nodes(tx, table_config: dict, records: List[Dict]):
    label = table_config["label"]
    
    for record in records:
        # 构建属性（排除 None 值）
        props = {k: sanitize_value(v) for k, v in record.items() if v is not None}
        query = f"CREATE (n:`{label}` $props)"
        tx.run(query, props=props)
    logger.info(f"✅ 创建 {len(records)} 个 {label} 节点")

def create_nodes_ex(tx, table_config: dict, records: List[Dict]):
    label = table_config["label"]
    id_fields = table_config["id_fields"]  # 如 ["GD_GUID"]

    for record in records:
        # 构建属性（排除 None）
        props = {k: v for k, v in record.items() if v is not None}

        # 选择一个非空的 ID 字段用于 MERGE（优先顺序按 id_fields）
        merge_key = None
        merge_value = None
        for field in id_fields:
            if field in props and props[field] is not None:
                merge_key = field
                merge_value = props[field]
                break

        if merge_key is None:
            # 如果没有有效 ID，跳过或使用全部属性（不推荐）
            logger.warning(f"⚠️ 跳过无唯一ID的记录: {props}")
            continue

        # 构建 MERGE 查询：仅用唯一键匹配，其他属性 ON CREATE SET
        query = f"""
        MERGE (n:`{label}` {{{merge_key}: $merge_value}})
        ON CREATE SET n += $other_props
        """
        other_props = {k: v for k, v in props.items() if k != merge_key}
        tx.run(query, merge_value=merge_value, other_props=other_props)

    logger.info(f"✅ 处理 {len(records)} 条 {label} 记录（已去重）")

# === 3. 创建关系（基于字段值匹配）===
def create_relationships(tx, rel_rule: tuple, all_data: dict):
    src_table, src_field, tgt_table, tgt_field, rel_type = rel_rule
    
    src_records = all_data[src_table]
    tgt_records = all_data[tgt_table]
    
    # 构建目标表的索引：{value -> [nodes]}
    tgt_index = {}
    for rec in tgt_records:
        key = rec.get(tgt_field)
        if key:
            tgt_index.setdefault(key, []).append(rec)
    
    count = 0
    for src_rec in src_records:
        src_val = src_rec.get(src_field)
        if not src_val:
            continue
        if src_val in tgt_index:
            for tgt_rec in tgt_index[src_val]:
                # 使用唯一标识（如 GUID）匹配，这里用整个记录内容作为属性
                # 实际中建议添加唯一ID字段（如 pg 的 ctid 或业务主键）
                query = f"""
                MATCH (a:{src_table.upper()} {{{src_field}: $src_val}})
                MATCH (b:{tgt_table.upper()} {{{tgt_field}: $tgt_val}})
                MERGE (a)-[:`{rel_type}`]->(b)
                """
                tx.run(query, src_val=src_val, tgt_val=src_val)
                count += 1
    
    logger.info(f"✅ 创建 {count} 条 {rel_type} 关系")

def fetch_spatial_overlaps(
    pg_config: dict,
    table_a: str,
    geom_field_a: str,
    id_field_a: str,
    table_b: str,
    geom_field_b: str,
    id_field_b: str
) -> List[Dict[str, Any]]:
    """
    查询两个表的空间重叠记录。
    :param table_a: 表A名称
    :param geom_field_a: 表A的空间字段名
    :param table_b: 表B名称
    :param geom_field_b: 表B的空间字段名
    :return: 重叠记录列表，包含 a.*, b.*, overlap_area
    """
    conn = psycopg2.connect(**pg_config)
    cur = conn.cursor()

    # 构建 SQL，使用传入的空间字段名
    alias_a = f"{table_a.upper()}_{id_field_a.upper()}"
    alias_b = f"{table_b.upper()}_{id_field_b.upper()}"
    sql = f"""
    SELECT 
        a."{id_field_a}" AS "{alias_a}",
        b."{id_field_b}" AS "{alias_b}",
        ST_Area(ST_Intersection(a.{geom_field_a}, b.{geom_field_b})) AS overlap_area
    FROM {table_a} a, {table_b} b
    WHERE 
        ST_Intersects(a.{geom_field_a}, b.{geom_field_b})
        AND ST_Area(ST_Intersection(a.{geom_field_a}, b.{geom_field_b})) > 0
    """

    try:
        cur.execute(sql)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        logger.info(f"🌍 空间叠加: {table_a}.{geom_field_a} × {table_b}.{geom_field_b} → 找到 {len(results)} 个重叠")
    except Exception as e:
        logger.error(f"❌ 空间查询失败 ({table_a} × {table_b}): {e}")
        results = []
    finally:
        cur.close()
        conn.close()

    return results

def create_spatial_relationships(tx, rel_config: tuple, all_table_configs: dict, overlap_records: List[Dict]):
    table_a, table_b, rel_type = rel_config

    # 获取标签和唯一字段
    cfg_a = next(t for t in all_table_configs if t["name"] == table_a)
    cfg_b = next(t for t in all_table_configs if t["name"] == table_b)

    label_a = cfg_a["label"]
    label_b = cfg_b["label"]

    # 获取唯一ID字段（优先用 id_uniq，否则用 id_fields[0]）
    def get_uniq_field(cfg):
        if "id_uniq" in cfg and cfg["id_uniq"]:
            return cfg["id_uniq"]
        elif cfg["id_fields"]:
            return cfg["id_fields"][0]
        else:
            raise ValueError(f"表 {cfg['name']} 无唯一ID字段")

    id_field_a = get_uniq_field(cfg_a)
    id_field_b = get_uniq_field(cfg_b)
    count = 0
    for rec in overlap_records:
        a_id = rec.get(id_field_a)
        b_id = rec.get(id_field_b)
        area = rec.get("overlap_area")

        if a_id is None or b_id is None or area is None:
            continue

        query = f"""
        MATCH (a:`{label_a}` {{{id_field_a}: $a_id}})
        MATCH (b:`{label_b}` {{{id_field_b}: $b_id}})
        MERGE (a)-[r:`{rel_type}`]->(b)
        ON CREATE SET r.overlap_area = $area
        ON MATCH SET r.overlap_area = $area  // 可选：是否允许更新面积
        """
        logger.info(f"uniq id a: {id_field_a}: {a_id}, uniq id b: {id_field_b}: {b_id}, area: {area} ")
        tx.run(query, a_id=a_id, b_id=b_id, area=float(area))
        count += 1

    logger.info(f"✅ 创建 {count} 条空间关系 {rel_type}（含 overlap_area）")

def create_spatial_relationships_ex(tx, rel_config: tuple, all_table_configs: dict, overlap_records: List[Dict]):
    table_a, table_b, rel_type = rel_config

    cfg_a = next(t for t in all_table_configs if t["name"] == table_a)
    cfg_b = next(t for t in all_table_configs if t["name"] == table_b)

    label_a = cfg_a["label"]
    label_b = cfg_b["label"]

    def get_uniq_field(cfg):
        if cfg.get("id_uniq"):
            return cfg["id_uniq"]
        elif cfg["id_fields"]:
            return cfg["id_fields"][0]
        else:
            raise ValueError(f"表 {cfg['name']} 无唯一ID字段")

    id_field_a = get_uniq_field(cfg_a)
    id_field_b = get_uniq_field(cfg_b)

    created_count = 0
    attempted_count = 0
    node_missing_count = 0
    # 先检查节点是否存在（调试用）
    # for i, rec in enumerate(overlap_records[:5]):  # 只检查前5条
    #     a_id = rec.get(f"{label_a}_{id_field_a}")
    #     b_id = rec.get(f"{label_b}_{id_field_b}")
    #     area = rec.get("overlap_area")

    #     if a_id is None or b_id is None or area is None:
    #         logger.info(f"存在空的id: {label_a}, {a_id}, {b_id}, {label_b}")
    #         continue
        
    #     check_query = f"""
    #     MATCH (a:`{label_a}` {{{id_field_a}: $a_id}})
    #     MATCH (b:`{label_b}` {{{id_field_b}: $b_id}})
    #     RETURN a IS NOT NULL AS a_exists, b IS NOT NULL AS b_exists
    #     """
    #     try:
    #         result = tx.run(check_query, a_id=a_id, b_id=b_id)
    #         record = result.single()
    #         if record:
    #             logger.debug(f"测试 {i+1}: {label_a}.{id_field_a}={a_id} 存在: {record['a_exists']}, {label_b}.{id_field_b}={b_id} 存在: {record['b_exists']}")
    #         else:
    #             logger.debug(f"测试 {i+1}: 未找到任何节点")
    #     except Exception as e:
    #         logger.error(f"❌ 检查节点失败: {e}")

    alias_a = f"{table_a.upper()}_{id_field_a.upper()}"
    alias_b = f"{table_b.upper()}_{id_field_b.upper()}"

    for rec in overlap_records:
        # a_id = rec.get(f"{label_a}_{id_field_a}")
        # b_id = rec.get(f"{label_b}_{id_field_b}")
        a_id = rec.get(alias_a)
        b_id = rec.get(alias_b)
        area = rec.get("overlap_area")

        if a_id is None or b_id is None or area is None:
            continue

        attempted_count += 1
        query = f"""
        MATCH (a:`{label_a}` {{{id_field_a}: $a_id}})
        MATCH (b:`{label_b}` {{{id_field_b}: $b_id}})
        MERGE (a)-[r:`{rel_type}`]->(b)
        ON CREATE SET r.overlap_area = $area
        ON MATCH SET r.overlap_area = $area
        """

        try:
            result = tx.run(query, a_id=a_id, b_id=b_id, area=float(area))
            summary = result.consume()  # 获取执行摘要
            # print(f"summary: {summary.counters}")
            # 详细日志
            logger.debug(f"查询: a={label_a}.{id_field_a}={a_id}, b={label_b}.{id_field_b}={b_id}")
            logger.debug(f"摘要: {summary.counters}")

            # if summary.counters.nodes_matched < 2:
                # node_missing_count += 1
                # logger.debug(f"⚠️ 节点匹配不足: {summary.counters.nodes_matched} 个节点（需要2个）")
            
            if summary.counters.relationships_created > 0:
                created_count += 1
                logger.debug(f"✅ 创建了新关系")

            relationships_created = summary.counters.relationships_created
            if relationships_created > 0:
                created_count += 1
            # 可选：如果想看每条是否命中节点（调试用）
            # nodes_matched = summary.counters.contains_updates  # 不准确
        except Exception as e:
            logger.error(
                f"❌ 创建空间关系失败: "
                f"{label_a}.{id_field_a}={a_id} → {label_b}.{id_field_b}={b_id}, "
                f"错误: {str(e)}"
            )
            continue

    logger.info(
        f"✅ 尝试创建 {attempted_count} 条 {rel_type} 关系，"
        f"实际新增 {created_count} 条（其余为已存在或节点未匹配）"
    )

def get_uniq_field(cfg):
    if cfg.get("id_uniq"):
        return cfg["id_uniq"]
    return cfg["id_fields"][0]

# === 主函数 ===
def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    # 1. 读取所有表数据
    all_data = {}
    for tbl in TABLES:
        logger.info(f"📥 读取表: {tbl['name']}")
        all_data[tbl['name']] = fetch_table_data(PG_CONFIG, tbl['name'])
    
    # 2. 导入 Neo4j
    with driver.session() as session:
        # 清空（可选）
        session.run("MATCH (n) DETACH DELETE n")
        
        # # 创建节点
        for tbl in TABLES:
            session.execute_write(create_nodes, tbl, all_data[tbl['name']])
        
        # 创建关系
        for rel in RELATIONSHIPS:
            session.execute_write(create_relationships, rel, all_data)
        
        # === 新增：创建空间叠加关系 ===
        for spatial_rel in SPATIAL_RELATIONSHIPS:
            table_a, geom_a, table_b, geom_b, rel_type = spatial_rel
            logger.info(f"🔍 执行空间叠加分析: {table_a}.{geom_a} ↔ {table_b}.{geom_b}")

            cfg_a = next(t for t in TABLES if t["name"] == table_a)
            cfg_b = next(t for t in TABLES if t["name"] == table_b)
            id_field_a = get_uniq_field(cfg_a)
            id_field_b = get_uniq_field(cfg_b)
            
            overlaps = fetch_spatial_overlaps(
                PG_CONFIG,
                table_a, geom_a, id_field_a,
                table_b, geom_b, id_field_b
            )
            
            session.execute_write(
                create_spatial_relationships_ex,
                (table_a, table_b, rel_type),  # 保持原格式传给 create_spatial_relationships
                TABLES,
                overlaps
            )
    
    driver.close()
    logger.info("🎉 数据血缘图谱构建完成！")

if __name__ == "__main__":
    main()
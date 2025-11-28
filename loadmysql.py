import pymysql
import time
from faker import Faker
import random
from datetime import datetime, timedelta

# -------------------------- 数据库连接配置（请修改为你的MySQL信息）--------------------------
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "mysql2",  # 需提前创建该数据库（CREATE DATABASE bank_data;）
    "charset": "utf8mb4",
    "port": 3306
}

# -------------------------- 初始化虚拟数据生成器 --------------------------
fake = Faker("zh_CN")
Faker.seed(42)  # 固定种子，保证数据可复现

# -------------------------- 数据字典（严格遵循PDF定义）--------------------------
# 证件类型
ID_TYPE_MAP = {"01": "身份证", "02": "护照"}
# 客户性别
GENDER_MAP = {"1": "男", "2": "女"}
# 客户风险等级
RISK_LEVEL_MAP = {"01": "低", "02": "中", "03": "高"}
# 客户状态 - 标识客户整体关系状态，与账户状态概念不同，建议保留
CUST_STATUS_MAP = {"0": "正常", "1": "冻结", "9": "销户"}
# 产品大类
PROD_CAT_MAP = {"DEP": "存款", "LN": "贷款"}
# 产品子类（存款）
DEP_SUB_CAT_MAP = {"DEP01": "活期", "DEP02": "定期"}
# 产品子类（贷款）
LN_SUB_CAT_MAP = {"PDL01": "房贷", "PDL02": "消费贷"}
# 产品风险等级
PROD_RISK_MAP = {"01": "PR1", "02": "PR2", "03": "PR3", "04": "PR4", "05": "PR5"}
# 产品状态
PROD_STATUS_MAP = {"0": "在售", "1": "停售", "2": "已下市"}
# 账户类型
ACCT_TYPE_MAP = {"101": "活期", "201": "定期"}
# 账户状态
ACCT_STATUS_MAP = {"0": "正常", "1": "冻结", "2": "挂失", "9": "销户"}
# 货币代码
CCY_CD_MAP = ["CNY"]
# 贷款类型
LN_TYPE_MAP = {"PDL": "个人贷款", "CDL": "企业贷款"}
# 还款方式
REPAY_MODE_MAP = {"001": "等额本息", "002": "等额本金"}
# 贷款状态
LN_STATUS_MAP = {"01": "正常", "02": "逾期", "05": "结清"}
# 五级分类
CLASS_5_MAP = {"01": "正常", "02": "关注", "03": "次级"}

# -------------------------- 表创建SQL（严格匹配PDF字段定义）--------------------------
CREATE_TABLE_SQLS = [
    # 客户信息表
    """
    CREATE TABLE IF NOT EXISTS customer_info (
        CUST_NO VARCHAR(20) NOT NULL COMMENT '客户号',
        CUST_NAM VARCHAR(100) NOT NULL COMMENT '客户姓名',
        ID_TYPE VARCHAR(2) NOT NULL COMMENT '证件类型（01-身份证，02-护照）',
        ID_NO VARCHAR(50) NOT NULL COMMENT '证件号码（脱敏存储）',
        BIRTH_DT CHAR(8) NOT NULL COMMENT '出生日期（YYYYMMDD）',
        GENDER VARCHAR(1) NOT NULL COMMENT '性别（1-男，2-女）',
        MOBILE_N VARCHAR(11) NOT NULL COMMENT '手机号码（脱敏）',
        ADDRESS VARCHAR(200) NOT NULL COMMENT '联系地址',
        OCCUP_CD VARCHAR(6) NOT NULL COMMENT '职业代码',
        RISK_LEVEL VARCHAR(2) NOT NULL COMMENT '客户风险等级（01-低，02-中，03-高）',
        CUST_STATUS VARCHAR(1) NOT NULL COMMENT '客户状态（0-正常，1-冻结，9-销户）',
        OPEN_ORG VARCHAR(10) NOT NULL COMMENT '开户机构代码',
        CUST_TYPE VARCHAR(2) NOT NULL COMMENT '客户类型（"01-个人""02-企业"，用于筛选个人客户）',
        PRIMARY KEY (CUST_NO)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户信息表';
    """,
    # 产品信息表
    """
    CREATE TABLE IF NOT EXISTS product_info (
        PROD_CD VARCHAR(20) NOT NULL COMMENT '产品代码',
        PROD_NAM VARCHAR(100) NOT NULL COMMENT '产品名称',
        PROD_CAT VARCHAR(3) NOT NULL COMMENT '产品大类（DEP-存款，LN-贷款）',
        PROD_SUB_CAT VARCHAR(5) NOT NULL COMMENT '产品子类',
        PROD_RISK VARCHAR(2) NOT NULL COMMENT '产品风险等级',
        MIN_BUY_AMT DECIMAL(18,2) NOT NULL COMMENT '起购金额',
        PROD_TERM INT(5) COMMENT '产品期限（天）',
        EXP_YR_RATE DECIMAL(6,4) NOT NULL COMMENT '预期年化收益率（%）',
        PROD_STATUS VARCHAR(1) NOT NULL COMMENT '产品状态（0-在售，1-停售，2-已下市）',
        PRIMARY KEY (PROD_CD)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产品信息表';
    """,
    # 存款业务表
    """
    CREATE TABLE IF NOT EXISTS deposit_business (
        ACCT_NO VARCHAR(25) NOT NULL COMMENT '账号',
        CUST_NO VARCHAR(20) NOT NULL COMMENT '客户号（关联customer_info）',
        PROD_CD VARCHAR(20) NOT NULL COMMENT '产品代码（关联product_info）',
        ACCT_TYPE VARCHAR(3) NOT NULL COMMENT '账户类型（101-活期，201-定期）',
        ACCT_STATUS VARCHAR(1) NOT NULL COMMENT '账户状态',
        CCY_CD VARCHAR(3) NOT NULL COMMENT '货币代码',
        CUR_BAL DECIMAL(18,2) NOT NULL COMMENT '活期余额',
        FIX_BAL DECIMAL(18,2) NOT NULL COMMENT '定期余额',
        FROZEN_AMT DECIMAL(18,2) NOT NULL COMMENT '冻结金额',
        OPEN_DT CHAR(8) NOT NULL COMMENT '开户日期（YYYYMMDD）',
        LAST_TRN_DT CHAR(8) NOT NULL COMMENT '最后交易日期（YYYYMMDD）',
        MTH_AVG_BAL DECIMAL(18,2) NOT NULL COMMENT '月日均余额',
        MATURITY_DT CHAR(8) COMMENT '定期存款到期日（用于产品到期集中度计算）',
        PRIMARY KEY (ACCT_NO),
        FOREIGN KEY (CUST_NO) REFERENCES customer_info(CUST_NO),
        FOREIGN KEY (PROD_CD) REFERENCES product_info(PROD_CD)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='存款业务表';
    """,
    # 贷款业务表
    """
    CREATE TABLE IF NOT EXISTS loan_business (
        DUE_BILL_NO VARCHAR(30) NOT NULL COMMENT '借据号',
        LN_ACCT_NO VARCHAR(25) NOT NULL COMMENT '贷款账号',
        CUST_NO VARCHAR(20) NOT NULL COMMENT '客户号（关联customer_info）',
        PROD_CD VARCHAR(20) NOT NULL COMMENT '产品代码（关联product_info）',
        LN_TYPE VARCHAR(3) NOT NULL COMMENT '贷款类型',
        LN_VARIETY VARCHAR(5) NOT NULL COMMENT '贷款品种',
        CONT_AMT DECIMAL(18,2) NOT NULL COMMENT '合同金额',
        LN_BAL DECIMAL(18,2) NOT NULL COMMENT '贷款余额',
        DISB_DT CHAR(8) NOT NULL COMMENT '放款日期（YYYYMMDD）',
        MATURITY_DT CHAR(8) NOT NULL COMMENT '到期日期（YYYYMMDD）',
        INT_RATE DECIMAL(6,4) NOT NULL COMMENT '执行利率（%）',
        REPAY_MODE VARCHAR(3) NOT NULL COMMENT '还款方式',
        LN_STATUS VARCHAR(2) NOT NULL COMMENT '贷款状态',
        OVERDUE_DAYS INT(5) NOT NULL COMMENT '逾期天数',
        CLASS_5 VARCHAR(2) NOT NULL COMMENT '五级分类',
        MTH_DUE_AMT DECIMAL(18,2) NOT NULL COMMENT '本月应还金额',
        PRIMARY KEY (DUE_BILL_NO),
        FOREIGN KEY (CUST_NO) REFERENCES customer_info(CUST_NO),
        FOREIGN KEY (PROD_CD) REFERENCES product_info(PROD_CD)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='贷款业务表';
    """,
    # 1. 机构与区域关联表
    """
    CREATE TABLE IF NOT EXISTS org_region (
        ORG_CD VARCHAR(10) NOT NULL COMMENT '机构代码（关联customer_info.OPEN_ORG）',
        BRANCH_NAME VARCHAR(50) NOT NULL COMMENT '分行名称（如 "烟台分行"）',
        REGION VARCHAR(20) NOT NULL COMMENT '所属区域（如 "山东省"）',
        PRIMARY KEY (ORG_CD)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='机构与区域关联表';
    """,
    # 2. 客户交易明细表
    """
    CREATE TABLE IF NOT EXISTS customer_transaction (
        TRANS_ID VARCHAR(30) NOT NULL COMMENT '交易唯一标识',
        CUST_NO VARCHAR(20) NOT NULL COMMENT '客户号（关联customer_info.CUST_NO）',
        ACCT_NO VARCHAR(25) NOT NULL COMMENT '交易账户（关联deposit_business.ACCT_NO或新增信用卡/理财账户）',
        TRANS_DT CHAR(8) NOT NULL COMMENT '交易日期（YYYYMMDD）',
        TRANS_AMT DECIMAL(18,2) NOT NULL COMMENT '交易金额（需脱敏）',
        TRANS_TYPE VARCHAR(10) NOT NULL COMMENT '交易类型（如 "存款""取款""转账"）',
        CHANNEL VARCHAR(20) NOT NULL COMMENT '交易渠道（"手机银行""柜面""ATM""微信" 等）',
        TRANS_STATUS VARCHAR(2) NOT NULL COMMENT '交易状态（"成功""失败"）',
        PRIMARY KEY (TRANS_ID),
        FOREIGN KEY (CUST_NO) REFERENCES customer_info(CUST_NO),
        FOREIGN KEY (ACCT_NO) REFERENCES deposit_business(ACCT_NO)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户交易明细表';
    """,

    # 4. 客户产品持有汇总表
    """
    CREATE TABLE IF NOT EXISTS customer_product_hold (
        CUST_NO VARCHAR(20) NOT NULL COMMENT '客户号',
        PROD_COUNT INT NOT NULL COMMENT '持有产品总数（存款、贷款、理财、信用卡等）',
        DEP_PROD_COUNT INT NOT NULL COMMENT '存款产品数量',
        LN_PROD_COUNT INT NOT NULL COMMENT '贷款产品数量',
        FIN_PROD_COUNT INT NOT NULL COMMENT '理财产品数量（新增类型）',
        CC_PROD_COUNT INT NOT NULL COMMENT '信用卡数量（新增类型）',
        EXPIRY_30D_AMT DECIMAL(18,2) NOT NULL COMMENT '未来 30 天内到期产品总金额（关联定期存款/理财到期日）',
        PRIMARY KEY (CUST_NO),
        FOREIGN KEY (CUST_NO) REFERENCES customer_info(CUST_NO)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户产品持有汇总表';
    """,
    # 5. 客户渠道偏好表
    """
    CREATE TABLE IF NOT EXISTS customer_channel_preference (
        CUST_NO VARCHAR(20) NOT NULL COMMENT '客户号',
        MB_TRANS_RATE DECIMAL(6,4) NOT NULL COMMENT '手机银行交易占比（近 3 个月）',
        COUNTER_TRANS_RATE DECIMAL(6,4) NOT NULL COMMENT '柜面交易占比（近 3 个月）',
        CHANNEL_ACTIVE_SCORE INT NOT NULL COMMENT '渠道活跃度评分（0-100，综合各渠道频次）',
        PRIMARY KEY (CUST_NO),
        FOREIGN KEY (CUST_NO) REFERENCES customer_info(CUST_NO)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户渠道偏好表';
    """,
    # 6. 营销活动与策略执行表
    """
    CREATE TABLE IF NOT EXISTS marketing_campaign (
        CAMPAIGN_ID VARCHAR(20) NOT NULL COMMENT '活动唯一标识',
        CUST_NO VARCHAR(20) NOT NULL COMMENT '目标客户号',
        STRATEGY_TYPE VARCHAR(50) NOT NULL COMMENT '策略类型（如 "工资代发签约 + 活期理财"）',
        PUSH_CHANNEL VARCHAR(20) NOT NULL COMMENT '推送渠道（"CRM 系统""手机银行"）',
        EXECUTE_DT CHAR(8) NOT NULL COMMENT '执行日期',
        RESPONSE_STATUS VARCHAR(20) NOT NULL COMMENT '客户响应（"接受""拒绝""未响应"）',
        AUM_CHANGE DECIMAL(18,2) NOT NULL COMMENT '策略执行后 AUM 变动（需脱敏）',
        PRIMARY KEY (CAMPAIGN_ID),
        FOREIGN KEY (CUST_NO) REFERENCES customer_info(CUST_NO)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='营销活动与策略执行表';
    """,
    # 7. 客户扩展信息表
    """
    CREATE TABLE IF NOT EXISTS customer_extend_info (
        CUST_NO VARCHAR(20) NOT NULL COMMENT '客户号（关联customer_info.CUST_NO）',
        MARITAL_STATUS VARCHAR(10) NOT NULL COMMENT '婚姻状况（"已婚""单身""离婚" 等）',
        EDUCATION VARCHAR(20) NOT NULL COMMENT '教育程度（"高中""大学""研究生" 等）',
        CREDIT_DEFAULT VARCHAR(1) NOT NULL COMMENT '信用违约记录（"是""否"）',
        HOUSING_LOAN VARCHAR(1) NOT NULL COMMENT '住房贷款持有（"是""否"）',
        SALARY_PAYMENT VARCHAR(1) NOT NULL COMMENT '是否签约工资代发（"是""否"）',
        PRIMARY KEY (CUST_NO),
        FOREIGN KEY (CUST_NO) REFERENCES customer_info(CUST_NO)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户扩展信息表';
    """,
    # 8. 理财产品信息表
    """
    CREATE TABLE IF NOT EXISTS finance_product_info (
        FIN_PROD_CD VARCHAR(20) NOT NULL COMMENT '理财代码',
        FIN_PROD_NAM VARCHAR(100) NOT NULL COMMENT '产品名称',
        RISK_LEVEL VARCHAR(2) NOT NULL COMMENT '风险等级（关联PROD_RISK_MAP）',
        REDEMPTION_DT CHAR(8) NOT NULL COMMENT '赎回日期（用于到期集中度计算）',
        PRIMARY KEY (FIN_PROD_CD)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='理财产品信息表';
    """,
    # 9. 用户月存款变化信息表
    """
    CREATE TABLE IF NOT EXISTS customer_monthly_deposit_change (
        ID INT AUTO_INCREMENT NOT NULL COMMENT '主键ID',
        CUST_NO VARCHAR(20) NOT NULL COMMENT '客户号（关联customer_info.CUST_NO）',
        MONTH VARCHAR(7) NOT NULL COMMENT '月份（格式：2024/MM）',
        DEPOSIT_AMT DECIMAL(18,2) NOT NULL COMMENT '月存款金额（已脱敏）',
        CHANGE_AMT DECIMAL(18,2) NOT NULL COMMENT '环比变化金额（已脱敏）',
        CHANGE_RATE DECIMAL(10,4) NOT NULL COMMENT '环比变化率',
        CREATE_TIME DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        PRIMARY KEY (ID),
        FOREIGN KEY (CUST_NO) REFERENCES customer_info(CUST_NO),
        UNIQUE KEY idx_cust_month (CUST_NO, MONTH)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户月存款变化信息表';
    """
]

# -------------------------- 虚拟数据生成函数 --------------------------
def generate_customer_info(count=50):
    """生成客户信息"""
    data = []
    for i in range(count):
        # 客户号（20位字符以内：C + 时间戳后11位 + 随机6位数字）
        # 使用时间戳+随机数方式生成更唯一的ID，避免重复
        timestamp = str(int(time.time() * 1000))[-11:]  # 取时间戳的后11位
        random_suffix = str(fake.random_int(100000, 999999))  # 6位随机数
        cust_no = f"C{timestamp}{random_suffix}"[:20]  # 确保不超过20位
        # 姓名
        cust_nam = fake.name()
        # 证件类型
        id_type = random.choice(list(ID_TYPE_MAP.keys()))
        # 证件号码（脱敏）
        if id_type == "01":
            id_no = f"{fake.ssn()[:6]}********{fake.ssn()[-4:]}"
        else:
            id_no = f"E{fake.random_int(10000000, 99999999)}"
        # 出生日期（YYYYMMDD）
        birth_dt = fake.date_between(start_date="-50y", end_date="-18y").strftime("%Y%m%d")
        # 性别
        gender = random.choice(list(GENDER_MAP.keys()))
        # 手机号（脱敏）
        # 先将 random_int 生成的整数转为字符串，再切片
        mobile_n = f"1{random.choice(['3', '5', '7', '8', '9'])}*****{str(fake.random_int(1000, 9999))[:4]}"
        # 联系地址
        address = fake.address().replace("\n", "")[:200]
        # 职业代码
        occup_cd = f"2020{fake.random_int(10, 99)}"
        # 客户风险等级
        risk_level = random.choice(list(RISK_LEVEL_MAP.keys()))
        # 客户状态
        cust_status = random.choice(["0", "1"])  # 大概率正常/冻结，少用销户
        # 开户机构
        open_org = f"11000{fake.random_int(1000, 9999)}"
        # 客户类型
        cust_type = random.choice(["01", "02"])
        
        data.append((
            cust_no, cust_nam, id_type, id_no, birth_dt, gender,
            mobile_n, address, occup_cd, risk_level, cust_status, open_org, cust_type
        ))
    return data

def generate_product_info():
    """生成产品信息（固定产品，符合PDF分类）"""
    data = [
        # 存款产品
        ("DEP2024001", "活期存款", "DEP", "DEP01", "01", 0.00, None, 0.3500, "0"),
        ("DEP2024002", "3个月定期存款", "DEP", "DEP02", "01", 1000.00, 90, 1.2000, "0"),
        ("DEP2024003", "1年定期存款", "DEP", "DEP02", "02", 1000.00, 360, 1.5000, "0"),
        ("DEP2024004", "3年定期存款", "DEP", "DEP02", "02", 10000.00, 1095, 1.8000, "1"),
        # 贷款产品
        ("LN2024001", "个人住房贷款", "LN", "PDL01", "03", 0.00, None, 4.2000, "0"),
        ("LN2024002", "个人消费贷款", "LN", "PDL02", "04", 0.00, None, 6.8000, "0"),
        ("LN2024003", "企业经营贷款", "LN", "CDL01", "05", 100000.00, None, 5.5000, "0"),
        ("LN2024004", "旧版消费贷", "LN", "PDL02", "04", 0.00, None, 7.2000, "2"),
    ]
    return data

def generate_deposit_business(cust_list, prod_list, count=80):
    """生成存款业务（关联客户和产品）"""
    data = []
    cust_nos = [c[0] for c in cust_list]
    dep_prods = [p[0] for p in prod_list if p[2] == "DEP"]  # 只选存款产品
    
    for i in range(count):
        # 账号
        acct_no = f"{fake.random_int(6222020000000000000, 6222029999999999999)}"
        # 关联客户
        cust_no = random.choice(cust_nos)
        # 关联产品
        prod_cd = random.choice(dep_prods)
        # 账户类型
        acct_type = "101" if prod_cd.endswith("001") else "201"
        # 账户状态
        acct_status = random.choice(list(ACCT_STATUS_MAP.keys()))
        # 货币代码
        ccy_cd = random.choice(CCY_CD_MAP)
        # 余额（根据产品类型生成）
        if acct_type == "101":
            cur_bal = round(random.uniform(100.00, 50000.00), 2)
            fix_bal = 0.00
        else:
            cur_bal = 0.00
            fix_bal = round(random.uniform(1000.00, 1000000.00), 2)
        # 冻结金额
        frozen_amt = round(random.uniform(0.00, cur_bal * 0.3), 2) if cur_bal > 0 else 0.00
        # 开户日期
        open_date = fake.date_between(start_date="-5y", end_date="-1m")
        open_dt = open_date.strftime("%Y%m%d")
        # 最后交易日期
        last_trn_dt = fake.date_between(start_date=open_date, end_date="today").strftime("%Y%m%d")
        # 月日均余额
        mth_avg_bal = round(random.uniform(cur_bal * 0.5, cur_bal * 0.9) if cur_bal > 0 else fix_bal * 0.8, 2)
        # 定期存款到期日（仅定期账户有）
        if acct_type == "201":
            # 根据产品期限计算到期日
            prod_term = next((p[6] for p in prod_list if p[0] == prod_cd), 360)
            maturity_dt = (open_date + timedelta(days=prod_term)).strftime("%Y%m%d")
        else:
            maturity_dt = None
        
        data.append((
            acct_no, cust_no, prod_cd, acct_type, acct_status, ccy_cd,
            cur_bal, fix_bal, frozen_amt, open_dt, last_trn_dt, mth_avg_bal, maturity_dt
        ))
    return data

def generate_loan_business(cust_list, prod_list, count=30):
    """生成贷款业务（关联客户和产品）"""
    data = []
    cust_nos = [c[0] for c in cust_list]
    loan_prods = [p[0] for p in prod_list if p[2] == "LN"]  # 只选贷款产品
    
    for i in range(count):
        # 借据号
        due_bill_no = f"LN{fake.random_int(20240000000000000000, 20249999999999999999)}"
        # 贷款账号
        ln_acct_no = f"{fake.random_int(1100010000000000000, 1100019999999999999)}"
        # 关联客户
        cust_no = random.choice(cust_nos)
        # 关联产品
        prod_cd = random.choice(loan_prods)
        # 贷款类型
        ln_type = "PDL" if prod_cd.startswith("LN2024001") or prod_cd.startswith("LN2024002") else "CDL"
        # 贷款品种
        ln_variety = "PDL01" if prod_cd == "LN2024001" else "PDL02" if prod_cd == "LN2024002" else "CDL01"
        # 合同金额
        cont_amt = round(random.uniform(10000.00, 2000000.00), 2)
        # 贷款余额（小于合同金额）
        ln_bal = round(cont_amt * random.uniform(0.3, 0.95), 2)
        # 放款日期
        disb_date = fake.date_between(start_date="-3y", end_date="-1m")
        disb_dt = disb_date.strftime("%Y%m%d")
        # 到期日期（根据放款日期+5-30年）
        maturity_dt = (disb_date + timedelta(days=random.randint(1825, 10950))).strftime("%Y%m%d")
        # 执行利率
        int_rate = round(random.uniform(3.8000, 7.5000), 4)
        # 还款方式
        repay_mode = random.choice(list(REPAY_MODE_MAP.keys()))
        # 贷款状态
        ln_status = random.choice(["01", "02"])  # 正常/逾期
        # 逾期天数
        overdue_days = random.randint(0, 180) if ln_status == "02" else 0
        # 五级分类
        class_5 = "01" if ln_status == "01" else random.choice(["02", "03"])
        # 本月应还金额
        mth_due_amt = round(ln_bal * 0.005, 2) if ln_bal > 0 else 0.00
        
        data.append((
            due_bill_no, ln_acct_no, cust_no, prod_cd, ln_type, ln_variety,
            cont_amt, ln_bal, disb_dt, maturity_dt, int_rate, repay_mode,
            ln_status, overdue_days, class_5, mth_due_amt
        ))
    return data

def generate_org_region(cust_list):
    """生成机构与区域关联表数据"""
    data = []
    # 从客户信息中提取所有开户机构代码
    org_codes = set(c[11] for c in cust_list)
    
    # 区域和分行映射
    region_branch_map = {
        "山东省": ["烟台分行", "青岛分行", "济南分行"],
        "江苏省": ["南京分行", "苏州分行", "无锡分行"],
        "浙江省": ["杭州分行", "宁波分行", "温州分行"],
        "广东省": ["深圳分行", "广州分行", "东莞分行"],
        "北京市": ["北京分行", "北京海淀支行", "北京朝阳支行"]
    }
    
    # 将机构代码分配到不同区域和分行
    regions = list(region_branch_map.keys())
    branch_index = 0
    
    for org_code in org_codes:
        region = regions[branch_index % len(regions)]
        branches = region_branch_map[region]
        branch_name = branches[branch_index % len(branches)]
        branch_index += 1
        
        data.append((org_code, branch_name, region))
    
    return data

def generate_customer_transaction(cust_list, deposit_list, count=500):
    """生成客户交易明细表数据"""
    data = []
    cust_nos = [c[0] for c in cust_list]
    acct_nos = [d[0] for d in deposit_list]
    trans_types = ["存款", "取款", "转账", "消费", "理财购买", "还款"]
    channels = ["手机银行", "柜面", "ATM", "微信", "支付宝", "网银"]
    trans_statuses = ["成功", "失败"]
    
    for i in range(count):
        # 交易ID
        trans_id = f"TR{fake.random_int(20240000000000000000, 20249999999999999999)}"
        # 关联客户
        cust_no = random.choice(cust_nos)
        # 关联账户
        acct_no = random.choice(acct_nos)
        # 交易日期（最近1年内）
        trans_dt = fake.date_between(start_date="-1y", end_date="today").strftime("%Y%m%d")
        # 交易金额（脱敏：保留量级，隐藏具体值）
        # 这里生成的是实际金额，在查询时会进行脱敏处理
        trans_amt = round(random.uniform(100.00, 100000.00), 2)
        # 交易类型
        trans_type = random.choice(trans_types)
        # 交易渠道
        channel = random.choice(channels)
        # 交易状态
        trans_status = random.choice(trans_statuses)
        
        data.append((
            trans_id, cust_no, acct_no, trans_dt, trans_amt,
            trans_type, channel, trans_status
        ))
    return data



def generate_customer_product_hold(cust_list, deposit_list, loan_list):
    """生成客户产品持有汇总表数据"""
    data = []
    cust_nos = [c[0] for c in cust_list]
    
    # 统计每个客户的产品持有情况
    for cust_no in cust_nos:
        # 存款产品数量
        dep_prod_count = len([d for d in deposit_list if d[1] == cust_no])
        # 贷款产品数量
        ln_prod_count = len([l for l in loan_list if l[2] == cust_no])
        # 理财产品数量（模拟）
        fin_prod_count = random.randint(0, 3)
        # 信用卡数量（模拟）
        cc_prod_count = random.randint(0, 2)
        # 总产品数量
        prod_count = dep_prod_count + ln_prod_count + fin_prod_count + cc_prod_count
        # 未来30天内到期产品总金额（模拟）
        expiry_30d_amt = round(random.uniform(0.00, 500000.00), 2)
        
        data.append((
            cust_no, prod_count, dep_prod_count, ln_prod_count,
            fin_prod_count, cc_prod_count, expiry_30d_amt
        ))
    return data

def generate_customer_channel_preference(cust_list):
    """生成客户渠道偏好表数据"""
    data = []
    cust_nos = [c[0] for c in cust_list]
    
    for cust_no in cust_nos:
        # 手机银行交易占比
        mb_trans_rate = round(random.uniform(0.1, 0.9), 4)
        # 柜面交易占比
        counter_trans_rate = round(random.uniform(0.05, 0.5), 4)
        # 确保占比合理（这里简化处理，不要求总和为1）
        # 渠道活跃度评分
        channel_active_score = random.randint(0, 100)
        
        data.append((
            cust_no, mb_trans_rate, counter_trans_rate, channel_active_score
        ))
    return data

def generate_marketing_campaign(cust_list, count=100):
    """生成营销活动与策略执行表数据"""
    data = []
    cust_nos = [c[0] for c in cust_list]
    strategy_types = [
        "工资代发签约 + 活期理财", "定期存款优惠", "信用卡推广", 
        "个人贷款优惠", "理财产品推荐", "保险产品推荐"
    ]
    push_channels = ["CRM系统", "手机银行", "短信", "电话"]
    response_statuses = ["接受", "拒绝", "未响应"]
    
    for i in range(count):
        # 活动ID
        campaign_id = f"MC{fake.random_int(202400000000, 202499999999)}"
        # 目标客户
        cust_no = random.choice(cust_nos)
        # 策略类型
        strategy_type = random.choice(strategy_types)
        # 推送渠道
        push_channel = random.choice(push_channels)
        # 执行日期
        execute_dt = fake.date_between(start_date="-6m", end_date="today").strftime("%Y%m%d")
        # 客户响应
        response_status = random.choice(response_statuses)
        # AUM变动（脱敏）
        aum_change = round(random.uniform(-50000.00, 200000.00), 2)
        
        data.append((
            campaign_id, cust_no, strategy_type, push_channel,
            execute_dt, response_status, aum_change
        ))
    return data

def generate_customer_extend_info(cust_list):
    """生成客户扩展信息表数据"""
    data = []
    cust_nos = [c[0] for c in cust_list]
    marital_statuses = ["已婚", "单身", "离婚", "丧偶"]
    educations = ["高中", "专科", "大学", "研究生", "博士"]
    
    for cust_no in cust_nos:
        # 婚姻状况
        marital_status = random.choice(marital_statuses)
        # 教育程度
        education = random.choice(educations)
        # 信用违约记录
        credit_default = random.choice(["是", "否"])
        # 住房贷款持有
        housing_loan = random.choice(["是", "否"])
        # 是否签约工资代发
        salary_payment = random.choice(["是", "否"])
        
        data.append((
            cust_no, marital_status, education, credit_default,
            housing_loan, salary_payment
        ))
    return data

def generate_finance_product_info(count=10):
    """生成理财产品信息表数据"""
    data = []
    finance_products = [
        "天天宝活期理财", "稳健理财30天", "稳健理财90天",
        "进取理财180天", "进取理财365天", "指数增强理财",
        "债券基金理财", "混合基金理财", "股票基金理财", "QDII理财"
    ]
    
    for i in range(min(count, len(finance_products))):
        # 理财代码
        fin_prod_cd = f"FIN{20240001 + i}"
        # 产品名称
        fin_prod_nam = finance_products[i]
        # 风险等级
        risk_level = random.choice(list(PROD_RISK_MAP.keys()))
        # 赎回日期
        redemption_dt = fake.date_between(start_date="+1m", end_date="+3y").strftime("%Y%m%d")
        
        data.append((
            fin_prod_cd, fin_prod_nam, risk_level, redemption_dt
        ))
    return data

def generate_customer_monthly_deposit_change(cust_list, months=12):
    """生成用户月存款变化信息表数据，并进行脱敏处理"""
    data = []
    cust_nos = [c[0] for c in cust_list]
    
    # 生成2024年12个月的月份列表
    months_list = [f"2024/{str(i).zfill(2)}" for i in range(1, months+1)]
    
    for cust_no in cust_nos:
        # 为每个客户生成一个基础存款金额，范围在10000-500000之间
        base_deposit = round(random.uniform(10000.00, 500000.00), 2)
        prev_deposit = base_deposit
        
        for month in months_list:
            # 生成当月的随机波动因子，在-0.15到0.2之间
            fluctuation = random.uniform(-0.15, 0.2)
            
            # 计算当月存款金额
            deposit_amt = round(prev_deposit * (1 + fluctuation), 2)
            
            # 确保金额不会低于0
            deposit_amt = max(0.01, deposit_amt)
            
            # 计算变化金额
            change_amt = round(deposit_amt - prev_deposit, 2)
            
            # 计算变化率
            if prev_deposit > 0:
                change_rate = round(change_amt / prev_deposit, 4)
            else:
                change_rate = 0.0
            
            # 对存款金额和变化金额进行脱敏处理（保留整数部分的前两位，其余用*替代）
            # 脱敏规则：保留量级，隐藏具体值
            # 例如：12367 -> 12***，3213221 -> 32*****，123234 -> 12****
            # 注意：实际存储的是原始数值，这里我们在生成时对数值进行脱敏处理
            # 脱敏处理：保留前两位有效数字，其余替换为随机数
            # 这里我们采用乘随机因子的方式进行脱敏，但保持整体分布
            deposit_amt_desensitized = round(deposit_amt * random.uniform(0.95, 1.05), 2)
            change_amt_desensitized = round(change_amt * random.uniform(0.95, 1.05), 2)
            
            data.append((
                None,  # ID (自增)
                cust_no,
                month,
                deposit_amt_desensitized,
                change_amt_desensitized,
                change_rate
            ))
            
            # 更新上个月的存款金额，用于下月计算
            prev_deposit = deposit_amt
    
    return data

# -------------------------- 数据导入函数 --------------------------
def import_data_to_mysql():
    try:
        # 连接数据库
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("数据库连接成功！")

        # 先删除已存在的表，确保新字段能被正确创建
        # 按照依赖关系重新排序，先删除引用其他表的表，再删除被引用的表
        tables_to_drop = [
            # 第一层：引用其他表的表
            'customer_monthly_balance',  # 引用customer_info和deposit_business
            'customer_transaction',  # 引用customer_info和deposit_business
            'customer_product_hold',  # 可能引用customer_info、deposit_business和loan_business
            
            # 第二层：业务表
            'deposit_business',  # 引用customer_info和product_info
            'loan_business',  # 引用customer_info和product_info
            
            # 第三层：其他业务相关表
            'customer_channel_preference',  # 引用customer_info
            'marketing_campaign',  # 引用customer_info
            'customer_extend_info',  # 引用customer_info
            'customer_monthly_deposit_change',  # 引用customer_info
            'org_region',  # 独立表
            'finance_product_info',  # 独立表
            
            # 最后删除基础表
            'product_info',  # 被多个表引用
            'customer_info'  # 被多个表引用
        ]
        
        for table in tables_to_drop:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"表 {table} 已删除")
            except Exception as e:
                print(f"删除表 {table} 时出错: {e}")
        conn.commit()
        
        # 1. 创建表
        for sql in CREATE_TABLE_SQLS:
            cursor.execute(sql)
        conn.commit()
        print("所有表创建成功！")

        # 2. 生成数据
        customer_data = generate_customer_info(count=100)
        product_data = generate_product_info()
        deposit_data = generate_deposit_business(customer_data, product_data, count=80)
        loan_data = generate_loan_business(customer_data, product_data, count=30)
        finance_product_data = generate_finance_product_info(10)
        org_region_data = generate_org_region(customer_data)
        customer_transaction_data = generate_customer_transaction(customer_data, deposit_data, 500)

        customer_product_hold_data = generate_customer_product_hold(customer_data, deposit_data, loan_data)
        customer_channel_preference_data = generate_customer_channel_preference(customer_data)
        marketing_campaign_data = generate_marketing_campaign(customer_data, 100)
        customer_extend_info_data = generate_customer_extend_info(customer_data)
        customer_monthly_deposit_change_data = generate_customer_monthly_deposit_change(customer_data, 12)
        print("虚拟数据生成成功！")

        # 3. 插入客户信息
        cust_sql = """
        INSERT INTO customer_info (CUST_NO, CUST_NAM, ID_TYPE, ID_NO, BIRTH_DT, GENDER, MOBILE_N, ADDRESS, OCCUP_CD, RISK_LEVEL, CUST_STATUS, OPEN_ORG, CUST_TYPE)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(cust_sql, customer_data)
        print(f"插入客户信息 {len(customer_data)} 条")

        # 4. 插入产品信息
        prod_sql = """
        INSERT INTO product_info (PROD_CD, PROD_NAM, PROD_CAT, PROD_SUB_CAT, PROD_RISK, MIN_BUY_AMT, PROD_TERM, EXP_YR_RATE, PROD_STATUS)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(prod_sql, product_data)
        print(f"插入产品信息 {len(product_data)} 条")

        # 5. 插入存款业务
        deposit_sql = """
        INSERT INTO deposit_business (ACCT_NO, CUST_NO, PROD_CD, ACCT_TYPE, ACCT_STATUS, CCY_CD, CUR_BAL, FIX_BAL, FROZEN_AMT, OPEN_DT, LAST_TRN_DT, MTH_AVG_BAL, MATURITY_DT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(deposit_sql, deposit_data)
        print(f"插入存款业务 {len(deposit_data)} 条")

        # 6. 插入贷款业务
        loan_sql = """
        INSERT INTO loan_business (DUE_BILL_NO, LN_ACCT_NO, CUST_NO, PROD_CD, LN_TYPE, LN_VARIETY, CONT_AMT, LN_BAL, DISB_DT, MATURITY_DT, INT_RATE, REPAY_MODE, LN_STATUS, OVERDUE_DAYS, CLASS_5, MTH_DUE_AMT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(loan_sql, loan_data)
        print(f"插入贷款业务 {len(loan_data)} 条")
        
        # 7. 插入理财产品信息
        finance_prod_sql = """
        INSERT INTO finance_product_info (FIN_PROD_CD, FIN_PROD_NAM, RISK_LEVEL, REDEMPTION_DT)
        VALUES (%s, %s, %s, %s)
        """
        cursor.executemany(finance_prod_sql, finance_product_data)
        print(f"插入理财产品信息 {len(finance_product_data)} 条")
        
        # 8. 插入机构与区域关联数据
        org_region_sql = """
        INSERT INTO org_region (ORG_CD, BRANCH_NAME, REGION)
        VALUES (%s, %s, %s)
        """
        cursor.executemany(org_region_sql, org_region_data)
        print(f"插入机构与区域关联数据 {len(org_region_data)} 条")
        
        # 9. 插入客户交易明细数据
        customer_transaction_sql = """
        INSERT INTO customer_transaction (TRANS_ID, CUST_NO, ACCT_NO, TRANS_DT, TRANS_AMT, TRANS_TYPE, CHANNEL, TRANS_STATUS)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(customer_transaction_sql, customer_transaction_data)
        print(f"插入客户交易明细数据 {len(customer_transaction_data)} 条")
        

        # 11. 插入客户产品持有汇总数据
        customer_product_hold_sql = """
        INSERT INTO customer_product_hold (CUST_NO, PROD_COUNT, DEP_PROD_COUNT, LN_PROD_COUNT, FIN_PROD_COUNT, CC_PROD_COUNT, EXPIRY_30D_AMT)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(customer_product_hold_sql, customer_product_hold_data)
        print(f"插入客户产品持有汇总数据 {len(customer_product_hold_data)} 条")
        
        # 12. 插入客户渠道偏好数据
        customer_channel_preference_sql = """
        INSERT INTO customer_channel_preference (CUST_NO, MB_TRANS_RATE, COUNTER_TRANS_RATE, CHANNEL_ACTIVE_SCORE)
        VALUES (%s, %s, %s, %s)
        """
        cursor.executemany(customer_channel_preference_sql, customer_channel_preference_data)
        print(f"插入客户渠道偏好数据 {len(customer_channel_preference_data)} 条")
        
        # 13. 插入营销活动数据
        marketing_campaign_sql = """
        INSERT INTO marketing_campaign (CAMPAIGN_ID, CUST_NO, STRATEGY_TYPE, PUSH_CHANNEL, EXECUTE_DT, RESPONSE_STATUS, AUM_CHANGE)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(marketing_campaign_sql, marketing_campaign_data)
        print(f"插入营销活动数据 {len(marketing_campaign_data)} 条")
        
        # 14. 插入客户扩展信息数据
        customer_extend_info_sql = """
        INSERT INTO customer_extend_info (CUST_NO, MARITAL_STATUS, EDUCATION, CREDIT_DEFAULT, HOUSING_LOAN, SALARY_PAYMENT)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(customer_extend_info_sql, customer_extend_info_data)
        print(f"插入客户扩展信息数据 {len(customer_extend_info_data)} 条")
        
        # 15. 插入客户月存款变化信息数据
        customer_monthly_deposit_change_sql = """
        INSERT INTO customer_monthly_deposit_change (CUST_NO, MONTH, DEPOSIT_AMT, CHANGE_AMT, CHANGE_RATE)
        VALUES (%s, %s, %s, %s, %s)
        """
        # 移除ID字段（自增）
        deposit_change_data = [(row[1], row[2], row[3], row[4], row[5]) for row in customer_monthly_deposit_change_data]
        cursor.executemany(customer_monthly_deposit_change_sql, deposit_change_data)
        print(f"插入客户月存款变化信息数据 {len(customer_monthly_deposit_change_data)} 条")

        # 提交事务
        conn.commit()
        print("所有数据导入成功！")

    except Exception as e:
        conn.rollback()
        print(f"数据导入失败：{str(e)}")
    finally:
        cursor.close()
        conn.close()
        print("数据库连接关闭！")

# -------------------------- 执行导入 --------------------------
if __name__ == "__main__":
    import_data_to_mysql()
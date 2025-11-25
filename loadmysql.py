import pymysql
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
# 客户状态
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
CCY_CD_MAP = ["CNY", "USD", "EUR"]
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
    """
]

# -------------------------- 虚拟数据生成函数 --------------------------
def generate_customer_info(count=50):
    """生成客户信息"""
    data = []
    for i in range(count):
        # 客户号（20位字符以内：C + 19位数字）
        cust_no = f"C{fake.random_int(1000000000000000000, 9999999999999999999)}"
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
        
        data.append((
            cust_no, cust_nam, id_type, id_no, birth_dt, gender,
            mobile_n, address, occup_cd, risk_level, cust_status, open_org
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
        
        data.append((
            acct_no, cust_no, prod_cd, acct_type, acct_status, ccy_cd,
            cur_bal, fix_bal, frozen_amt, open_dt, last_trn_dt, mth_avg_bal
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

# -------------------------- 数据导入函数 --------------------------
def import_data_to_mysql():
    try:
        # 连接数据库
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("数据库连接成功！")

        # 1. 创建表
        for sql in CREATE_TABLE_SQLS:
            cursor.execute(sql)
        conn.commit()
        print("所有表创建成功！")

        # 2. 生成数据
        customer_data = generate_customer_info(count=50)
        product_data = generate_product_info()
        deposit_data = generate_deposit_business(customer_data, product_data, count=80)
        loan_data = generate_loan_business(customer_data, product_data, count=30)
        print("虚拟数据生成成功！")

        # 3. 插入客户信息
        cust_sql = """
        INSERT INTO customer_info (CUST_NO, CUST_NAM, ID_TYPE, ID_NO, BIRTH_DT, GENDER, MOBILE_N, ADDRESS, OCCUP_CD, RISK_LEVEL, CUST_STATUS, OPEN_ORG)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        INSERT INTO deposit_business (ACCT_NO, CUST_NO, PROD_CD, ACCT_TYPE, ACCT_STATUS, CCY_CD, CUR_BAL, FIX_BAL, FROZEN_AMT, OPEN_DT, LAST_TRN_DT, MTH_AVG_BAL)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
from fastapi import FastAPI, Request
import httpx
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

AC_URL = "https://ntwmarkets.api-us1.com/api/3"
AC_TOKEN = "ba3dd3a9e627314b45d1045914b7502d0badf0e3bcb33a35ed182acda6c5345244ec4b55"
AC_HEADERS = {"Api-Token": AC_TOKEN, "Content-Type": "application/json"}

# Map B2Core field names to ActiveCampaign custom field IDs
FIELD_MAP = {
    "accountstatus": 2,
    "client_type": 3,
    "jurisdiction": 4,
    "country": 5,
    "verification_level": 6,
    "account_no": 7,
    "memberId": 8,
    "accountstatus": 9,
    "balance": 10,
    "base_currency": 11,
    "compliance_status": 12,
    "client_level": 13,
    "client_source": 14,
    "language": 15,
    "funded": 16,
    "countdeposits": 17,
    "countwithdrawals": 18,
    "first_deposit_date": 19,
    "ftd_amount": 20,
    "last_deposit_amount": 21,
    "payment_method": 22,
    "ftd": 23,
    "last_login": 24,
    "symbol": 25,
    "profit": 26,
    "transaction_date": 27,
    "amount": 28,
    "phone": 29,
    "city": 30,
    "address": 31,
    "post_code": 32,
    "birth_date": 33,
    "citizenship": 34,
    "resident": 35,
    "other_email": 36,
    "ui_language": 37,
    "accountid": 38,
    "login": 39,
    "subscriberId": 40,
    "partner_id": 41,
    "platform": 42,
    "group": 43,
    "department": 44,
    "balance_demo": 45,
    "credit": 46,
    "credit_demo": 47,
    "equity": 48,
    "equity_demo": 49,
    "open_pnl": 50,
    "open_pnl_demo": 51,
    "questionnaire_completed": 52,
    "sms_verified": 53,
    "phone_is_valid": 54,
    "compliance_status_modification": 55,
    "total_deposit": 56,
    "total_withdrawal": 57,
    "net_deposit": 58,
    "SumOfDeposits": 59,
    "last_deposit_date": 60,
    "countfees": 61,
    "qualified_ftd": 62,
    "qualified_ftd_date": 63,
    "first_trade_date": 64,
    "last_trade_date": 65,
    "last_interaction_date": 66,
    "last_login": 67,
    "last_comment": 68,
    "campaign": 69,
    "promocode": 70,
    "referral": 71,
    "client_experience": 72,
    "potential": 73,
    "retention_status": 74,
    "registration_date": 75,
    "registration_state": 76,
    "accept_promotions": 77,
    "param1": 78,
    "param2": 79,
    "param3": 80,
    "param4": 81,
    "param5": 82,
    "total_pnl": 83,
    "total_pnl_demo": 84,
    "trading_account_id": 85,
    "role": 86,
    "state": 87,
    "param6": 88,
    "param7": 89,
    "param8": 90,
    "param9": 91,
    "param10": 92,
}


async def upsert_contact(email: str, fields: dict, tags: list[str] = None):
    if not email:
        return

    field_values = []
    for b2c_key, ac_id in FIELD_MAP.items():
        if b2c_key in fields and fields[b2c_key] is not None:
            field_values.append({"field": str(ac_id), "value": str(fields[b2c_key])})

    payload = {"contact": {"email": email, "fieldValues": field_values}}

    async with httpx.AsyncClient() as client:
        r = await client.post(f"{AC_URL}/contact/sync", headers=AC_HEADERS, json=payload)
        logger.info(f"Contact sync {email}: {r.status_code}")
        contact_data = r.json()
        contact_id = contact_data.get("contact", {}).get("id")

        if tags and contact_id:
            for tag_name in tags:
                tag_r = await client.get(f"{AC_URL}/tags?search={tag_name}", headers=AC_HEADERS)
                tag_list = tag_r.json().get("tags", [])
                if tag_list:
                    tag_id = tag_list[0]["id"]
                else:
                    create_r = await client.post(f"{AC_URL}/tags", headers=AC_HEADERS, json={"tag": {"tag": tag_name, "tagType": "contact"}})
                    tag_id = create_r.json().get("tag", {}).get("id")
                if tag_id:
                    await client.post(f"{AC_URL}/contactTags", headers=AC_HEADERS, json={"contactTag": {"contact": contact_id, "tag": tag_id}})


@app.post("/webhook/new-subscriber")
async def new_subscriber(request: Request):
    data = await request.json()
    email = data.get("email")
    tags = ["signed-up"]
    if data.get("funded") == 1 or str(data.get("funded")) == "1":
        tags.append("funded")
    await upsert_contact(email, data, tags)
    return {"status": "ok"}


@app.post("/webhook/update-subscriber")
async def update_subscriber(request: Request):
    data = await request.json()
    email = data.get("email")
    tags = []
    if data.get("compliance_status") == "approved":
        tags.append("verified")
    if data.get("funded") == 1 or str(data.get("funded")) == "1":
        tags.append("funded")
    if data.get("retention_status"):
        tags.append(f"retention-{data['retention_status']}")
    await upsert_contact(email, data, tags)
    return {"status": "ok"}


@app.post("/webhook/deposit")
async def deposit(request: Request):
    data = await request.json()
    email = data.get("email")
    tags = ["deposited"]
    if str(data.get("ftd", "")).lower() == "yes" or data.get("ftd") == 1:
        tags.append("first-depositor")
    await upsert_contact(email, data, tags)
    return {"status": "ok"}


@app.post("/webhook/deposit-success")
async def deposit_success(request: Request):
    data = await request.json()
    email = data.get("email")
    await upsert_contact(email, data, ["deposit-success"])
    return {"status": "ok"}


@app.post("/webhook/real-position")
async def real_position(request: Request):
    data = await request.json()
    email = data.get("email")
    await upsert_contact(email, data, ["active-trader"])
    return {"status": "ok"}


@app.post("/webhook/demo-position")
async def demo_position(request: Request):
    data = await request.json()
    email = data.get("email")
    await upsert_contact(email, data, ["demo-trader"])
    return {"status": "ok"}


@app.post("/webhook/login")
async def login(request: Request):
    data = await request.json()
    email = data.get("email")
    await upsert_contact(email, data, ["logged-in"])
    return {"status": "ok"}


@app.post("/webhook/login-fail")
async def login_fail(request: Request):
    data = await request.json()
    email = data.get("email")
    await upsert_contact(email, data, ["login-failed"])
    return {"status": "ok"}


@app.post("/webhook/margin-call")
async def margin_call(request: Request):
    data = await request.json()
    email = data.get("email")
    await upsert_contact(email, data, ["margin-call"])
    return {"status": "ok"}


@app.post("/webhook/withdraw")
async def withdraw(request: Request):
    data = await request.json()
    email = data.get("email")
    await upsert_contact(email, data, ["withdrawal"])
    return {"status": "ok"}


@app.post("/webhook/signup-success")
async def signup_success(request: Request):
    data = await request.json()
    email = data.get("email")
    await upsert_contact(email, data, ["signed-up"])
    return {"status": "ok"}


@app.post("/webhook/logout")
async def logout(request: Request):
    data = await request.json()
    email = data.get("email")
    await upsert_contact(email, data, [])
    return {"status": "ok"}


@app.post("/webhook/credit-in")
async def credit_in(request: Request):
    data = await request.json()
    email = data.get("email")
    await upsert_contact(email, data, ["credit-in"])
    return {"status": "ok"}


@app.post("/webhook/credit-out")
async def credit_out(request: Request):
    data = await request.json()
    email = data.get("email")
    await upsert_contact(email, data, ["credit-out"])
    return {"status": "ok"}


@app.get("/health")
async def health():
    return {"status": "ok"}

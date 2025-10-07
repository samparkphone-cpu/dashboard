from dotenv import load_dotenv
load_dotenv()

import os
import logging
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException
from upload_handler import parse_excel  # should return [(name, phone), ...]
from db import get_connection  # your psycopg2 connection helper

app = FastAPI()
logger = logging.getLogger("uvicorn.error")

EDGE_URL = os.getenv("EDGE_URL")
EDGE_KEY = os.getenv("EDGE_KEY")


@app.post("/upload")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported.")

    try:
        contents = await file.read()
        rows = parse_excel(contents)

        conn = get_connection()
        cur = conn.cursor()

        if rows:
            args_str = ",".join(cur.mogrify("(%s,%s)", x).decode("utf-8") for x in rows)
            cur.execute(f"INSERT INTO phone_queue (name, phone_number) VALUES {args_str}")

        conn.commit()
        cur.close()
        conn.close()

        return {"message": f"✅ Inserted {len(rows)} rows into the database."}

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/start-calls")
async def start_calls(batch_limit: int = 200):
    """
    Dispatch pending calls and trigger Supabase Edge function.
    """
    conn = get_connection()
    batch_payload = []

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, phone_number 
                    FROM phone_queue 
                    WHERE status = 'pending'
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                    """,
                    (batch_limit,),
                )
                pending = cur.fetchall()

                if not pending:
                    return {"message": "No pending numbers to process."}

                for phone_id, phone_number in pending:
                    # Pick available Twilio number
                    cur.execute(
                        """
                        SELECT id, phone_number
                        FROM twilio_numbers
                        WHERE is_active = TRUE 
                          AND calls_made_today < daily_limit
                        ORDER BY calls_made_today ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """
                    )
                    tw = cur.fetchone()
                    if not tw:
                        logger.warning("No available Twilio numbers left.")
                        break

                    twilio_id, twilio_num = tw

                    # Create dispatch record
                    cur.execute(
                        """
                        INSERT INTO call_dispatch (phone_number, status, twilio_number_id, created_at)
                        VALUES (%s, 'queued', %s, now())
                        RETURNING id
                        """,
                        (phone_number, twilio_id),
                    )
                    dispatch_id = cur.fetchone()[0]

                    # Update Twilio usage + queue
                    cur.execute(
                        "UPDATE twilio_numbers SET calls_made_today = calls_made_today + 1 WHERE id = %s",
                        (twilio_id,),
                    )
                    cur.execute(
                        "UPDATE phone_queue SET status = 'dispatched' WHERE id = %s",
                        (phone_id,),
                    )

                    batch_payload.append(
                        {
                            "dispatch_id": dispatch_id,
                            "phone_number": phone_number,
                            "twilio_number": twilio_num,
                        }
                    )

        # --- Send batch to Supabase Edge ---
        if batch_payload:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {EDGE_KEY}",
            }

            async with httpx.AsyncClient(timeout=20.0) as client:
                resp = await client.post(EDGE_URL, json=batch_payload, headers=headers)

                try:
                    resp.raise_for_status()
                    edge_json = resp.json()
                except Exception as e:
                    logger.exception("Edge call failed")
                    raise HTTPException(status_code=502, detail=f"Edge error: {resp.text}")

            return {
                "message": f"✅ Started {len(batch_payload)} calls",
                "edge_response": edge_json,
            }

        else:
            return {"message": "No calls started (no Twilio numbers or no pending rows)."}

    except Exception as e:
        logger.exception("start_calls error")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

        
@app.post("/emergency-stop")
async def emergency_stop():
    """Immediately stop all calls by clearing the queue"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Delete all pending calls
        cur.execute("DELETE FROM phone_queue WHERE status = 'pending'")
        deleted_count = cur.rowcount
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {"message": f"🚨 EMERGENCY STOP: Deleted {deleted_count} pending calls"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

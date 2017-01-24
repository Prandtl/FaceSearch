import aiohttp
import asyncio
import json
import motor.motor_asyncio
from pprint import pprint
import uvloop
from parser import get_json_body
import sys

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def get_lat_and_lng(loc_id):
    url = 'https://www.instagram.com/explore/locations/%s/?__a=1' % loc_id
    with aiohttp.ClientSession() as session:
        data = await get_json_body(session, url)

        return data['location']['lat'], data['location']['lng']


async def put_db_locations_to_q(db, q):
    cursor = db.posts.find({
        "$and": [
                    {"location": {"$ne":None}}, 
                    {"location.lat" : { "$exists": False }} 
                ]
        }, 
        {"location":1})


    async for document in cursor:
        q.put_nowait(document)


async def handle_task(work_queue, db):
    with aiohttp.ClientSession() as session:
        while not work_queue.empty():
            document = await work_queue.get()
            lat, lng = await get_lat_and_lng(document['location']['id'])
            result = await db.posts.update(
                                {"_id": document['_id']}, 
                                {'$set': {
                                        'location.lat': lat,
                                        'location.lng': lng}})
            sys.stdout.write('\r%s' % str(document['_id'])[-1])
            sys.stdout.flush()

N_TASKS = 10

if __name__ == '__main__':
    client = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)
    db = client['instagram']
    loop = asyncio.get_event_loop()
    q = asyncio.Queue()
    loop.run_until_complete(put_db_locations_to_q(db, q))

    tasks = [handle_task(q, db) for _ in range(N_TASKS)]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
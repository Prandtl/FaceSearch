import aiohttp
import asyncio
import json
import motor.motor_asyncio

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from pprint import pprint

class PvivateProfile(Exception):
    pass

async def get_json_body(session, url):
    with aiohttp.Timeout(15):
        async with session.get(url) as response:
            r_text = await response.text()
            assert response.status == 200
            content = json.loads(r_text)
            return content

async def get_detailed_post_data(session, post_code):
    url = 'https://instagram.com/p/%s/?__a=1' % post_code
    data = await get_json_body(session, url)
    return data['media']


async def get_media(session, username, max_id=None):
    url = 'https://instagram.com/' + username + '/media'

    if max_id:
        url += '?&max_id=%s' % max_id

    media = await get_json_body(session, url)
    if not media['items']:
        raise PvivateProfile

    detailed_posts = []
    for post in media['items']:
        post_info = await get_detailed_post_data(session, post['code'])
        detailed_posts.append(post_info)

    media['items'] = detailed_posts
    return media

async def crawl(session: aiohttp.ClientSession, username):
    posts = []

    media = await get_media(session, username)
    posts += media['items']

    while media.get('more_available', False):

        max_id = media['items'][-1]['id']
        media = await get_media(session, username, max_id)
        posts += media['items']

    return posts

async def handle_task(work_queue, db):
    with aiohttp.ClientSession() as session:
        while not work_queue.empty():
            username = await work_queue.get()
            print(username)
            try:
                posts = await crawl(session, username)
            except PvivateProfile:
                print ('User %s is private' % username)
            except Exception as e:
                print ('%s %s'%(username, e))
            else:
                for post in posts:
                    await db.posts.insert(post)
                print ('%s %d'%(username, len(posts)))

async def get_usernames():

    with open('nn_strip.txt') as f:
        to_process = f.read().split('\n')
    in_base = await db.posts.distinct("owner.username")
    return set(to_process) - set(in_base)

def sync_getter(func):
    return asyncio.get_event_loop().run_until_complete(func())

N_TASKS = 10

if __name__ == '__main__':
    client = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)
    db = client['instagram']
    loop = asyncio.get_event_loop()
    
    q = asyncio.Queue()
    # one synchronous query
    usernames = sync_getter(get_usernames)
    [q.put_nowait(username) for username in usernames]
    tasks = [handle_task(q, db) for _ in range(N_TASKS)]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

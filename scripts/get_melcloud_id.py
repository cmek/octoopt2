import asyncio, aiohttp, pymelcloud
from pymelcloud import DEVICE_TYPE_ATW

async def list_devices():
    import os
    from dotenv import load_dotenv
    load_dotenv()
    async with aiohttp.ClientSession() as session:
        token = await pymelcloud.login(os.environ['MELCLOUD_EMAIL'], os.environ['MELCLOUD_PASSWORD'], session=session)
        devices = await pymelcloud.get_devices(token, session=session)
        for d in devices.get(DEVICE_TYPE_ATW, []):
            print(f'Device ID: {d.device_id}  Name: {d.name}')

asyncio.run(list_devices())

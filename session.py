import asyncio
import aiohttp
import logging

class GSession(aiohttp.ClientSession):
    """
    Make Session great again!
    Basically, this class allow to get page content without need of another await - await response.text()
    Also, request wrapped into semaphore context manager to explicitly limit number of active connections
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger('great_session')

    async def get(self, url, *, allow_redirects=True, semaphore=None, **kwargs):
        retries = 0
        while True:
            if semaphore:
                semaphore.locked()
            try:
                async with super().get(url, allow_redirects=True, **kwargs) as response:
                    content = await response.text()

                    response.content = content if content else None
                    return response
            except asyncio.TimeoutError:
                self.logger.error('Timeout at {}. Retry #{}'.format(url, retries))

                retries += 1
                if retries > 3:
                    break
                else:
                    if semaphore:
                        semaphore.release()
                    asyncio.sleep(2)
            finally:
                if semaphore:
                    semaphore.release()

    async def post(self, url, *, data, semaphore=None, **kwargs):
        if semaphore:
            semaphore.locked()
        try:
            async with super().post(url, data=data, **kwargs) as response:
                content = await response.text()

                response.content = content if content else None
                return response
        except asyncio.TimeoutError:
            self.logger.error('timeout error for url: {}'.format(url))
        finally:
            if semaphore:
                semaphore.release()

# async def main():
#     s = GSession()
#     res = await s.post('http://httpbin.org/post', data={'a': 1})
#     if res:
#         print(res.content)
#
# if __name__ == '__main__':
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(main())
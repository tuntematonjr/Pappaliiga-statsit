import asyncio
from db_async import readonly_connection

async def check():
    team_id = 'aa9d23f4-25e6-45cb-ba04-158250bd7efb'
    championship_id = '29b9e3aa-f28d-4e70-a21a-97b220dee784'
    
    async with readonly_connection() as conn:
        async with conn.cursor() as cur:
            # Check if team exists
            await cur.execute('SELECT team_id, name FROM teams WHERE team_id = %s', (team_id,))
            team = await cur.fetchone()
            print('Team:', team)
            
            # Check if championship exists
            await cur.execute('SELECT championship_id, name, season FROM championships WHERE championship_id = %s', (championship_id,))
            champ = await cur.fetchone()
            print('Championship:', champ)
            
            # Check if team has data for this championship
            await cur.execute('''
                SELECT COUNT(*) as cnt 
                FROM team_championships 
                WHERE team_id = %s AND champ_id = %s
            ''', (team_id, championship_id))
            result = await cur.fetchone()
            print('Team-Championship link:', result)
            
            # Check season totals
            await cur.execute('''
                SELECT matches_played, maps_played, maps_won, maps_lost 
                FROM team_season_totals 
                WHERE team_id = %s AND champ_id = %s
            ''', (team_id, championship_id))
            totals = await cur.fetchone()
            print('Team season totals:', totals if totals else 'No data')

asyncio.run(check())

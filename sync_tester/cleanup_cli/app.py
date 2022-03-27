"""
The main module to process automation cleanup
"""

import os
from sync_tester.cleanup_cli import postgres, s3

# endpoint_url = os.environ.get('AGENT_URL')
# if not endpoint_url:
#     print('AGENT_URL environment not provide -> upload cli with provided variable')

CLEANUP_SOURCE_DIR = '/home/ronenk1/dev/automation-sync-test/clean_test.json'
CLEAN_CONFIG = ''

menu = f"\nChoose one of the following Option:\n" \
       f"[1] - Configure cleanup data file (json)\n" \
       f"[2] - Show Data for deletion\n" \
       f"[3] - Execute Cleanup\n" \
       f"[4] - Configure environment file (json)\n" \
       f"[0] - Exit"


def exit_prog():
    opt = input('\n[9] -> for menu | exit with any other key: ')
    if opt == "9":
        print(menu)

    else:
        print('Will terminate running...')
        exit(0)


if __name__ == '__main__':

    print('This is MC data cleanup CLI')
    print('Multiple clean option of layers data on environment')
    agent_api = discrete_ingestion_executors.DiscreteAgentAdapter(endpoint_url)

    run = True
    print(menu)
    while run:
        choice = input(">> ")
        if choice == "0":
            print('Will terminate running...')
            exit(0)

        elif choice == "1":
            try:
                resp = agent_api.get_watch_status()
            except Exception as e:
                resp = str(e)

            print(f'[{resp}]')
            exit_prog()

        elif choice == "2":
            try:
                resp = agent_api.start_agent_watch()
            except Exception as e:
                resp = str(e)

            print(f'[{resp}]')
            exit_prog()

        elif choice == "3":
            try:
                resp = agent_api.stop_agent_watch()
            except Exception as e:
                resp = str(e)

            print(f'[{resp}]')
            exit_prog()

        elif choice == "4":
            path = input('Insert Layer relative path: ')
            try:
                resp = agent_api.send_agent_manual_ingest(path)
            except Exception as e:
                resp = str(e)

            print(f'[{resp}]')
            exit_prog()

        elif choice == "5":
            url = input('Please insert new url: ')
            agent_api = discrete_ingestion_executors.DiscreteAgentAdapter(url)
            print(f'New connection changed to Agent API with url: {url}\n')
            print(menu)

        else:
            print(f'Wrong key value was insert [{choice}]')
            exit_prog()




import requests
import sys
import json

def create_topic(url, topic):
    response = requests.put(f"{url}/topic", json={"topic": topic})
    return response.json()

def put_message(url, topic, message):
    response = requests.put(f"{url}/message", json={"topic": topic, "message": message})
    return response.json()

def get_message(url, topic):
    response = requests.get(f"{url}/message/{topic}")
    return response.json()

def get_topics(url):
    response = requests.get(f"{url}/topic")
    return response.json()

def get_status(url):
    response = requests.get(f"{url}/status")
    return response.json()

def main():
    if len(sys.argv) < 3:
        print("Usage: python client.py <url> <command> [<args>]")
        sys.exit(1)

    url = sys.argv[1]
    command = sys.argv[2]
    
    match command:
        case "create_topic":
            if len(sys.argv) != 4:
                print("Usage: python client.py <url> create_topic <topic>")
                sys.exit(1)
            topic = sys.argv[3]
            result = create_topic(url, topic)
            print(json.dumps(result, indent=2))

        case "put_message":
            if len(sys.argv) != 5:
                print("Usage: python client.py <url> put_message <topic> <message>")
                sys.exit(1)
            topic = sys.argv[3]
            message = sys.argv[4]
            result = put_message(url, topic, message)
            print(json.dumps(result, indent=2))

        case "get_message":
            if len(sys.argv) != 4:
                print("Usage: python client.py <url> get_message <topic>")
                sys.exit(1)
            topic = sys.argv[3]
            result = get_message(url, topic)
            print(json.dumps(result, indent=2))

        case "get_topics":
            result = get_topics(url)
            print(json.dumps(result, indent=2))

        case "get_status":
            result = get_status(url)
            print(json.dumps(result, indent=2))

        case _:
            print(f"Unknown command: {command}")
            sys.exit(1)

if __name__ == "__main__":
    main()
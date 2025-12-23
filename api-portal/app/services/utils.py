import random

ADJECTIVES = [
    "happy", "lucky", "sunny", "clever", "brave", "calm", "gentle", "jolly", "kind", "lively",
    "nice", "proud", "silly", "witty", "zealous", "alert", "bright", "cheery", "daring", "eager",
    "fancy", "giddy", "hearty", "jovial", "merry", "neat", "obedient", "polite", "quick", "rapid"
]

ANIMALS = [
    "otter", "badger", "bear", "beaver", "bison", "cat", "chimp", "cobra", "crane", "crow",
    "deer", "dog", "dolphin", "dove", "eagle", "falcon", "ferret", "fox", "frog", "goat",
    "goose", "hawk", "lion", "llama", "mole", "mouse", "owl", "panda", "puppy", "rabbit",
    "rat", "raven", "seal", "shark", "sheep", "sloth", "snake", "spider", "swan", "tiger",
    "toad", "trout", "turkey", "turtle", "whale", "wolf", "zebra"
]

def generate_readable_id(model_name: str) -> str:
    """
    Generates a readable ID like '{model_name}_{adjective}_{animal}{number}'.
    """
    adj = random.choice(ADJECTIVES)
    animal = random.choice(ANIMALS)
    number = random.randint(100, 999)
    
    # Sanitize model name: lowercase and replace spaces with underscores, keep alphanumeric/underscore
    safe_name = "".join(c for c in model_name.lower() if c.isalnum() or c == '_').rstrip()
    if not safe_name:
        safe_name = "model"
        
    return f"{safe_name}_{adj}_{animal}{number}"

from faker import Faker
from tqdm import tqdm, trange
from tqdm import trange
from num2words import num2words

def populate_set_and_save_chunks_of_size(numoflines: int) -> None:
    faker = Faker()
    chunksize: int = numoflines // 10 if numoflines <= 10000 else numoflines // 100
    numofchunks: int = numoflines // chunksize
    for _ in trange(numofchunks, total=numofchunks, unit_scale=True, unit=" chunk(s)", desc=f"writing {num2words(numoflines)} fake emails to {numofchunks} chunks of {chunksize}", colour="green"):
        tempset = {f"{faker.email(safe=True)}\n" for _ in trange(chunksize, total=chunksize, unit_scale=True,
                                                                 unit=" email(s)", desc=f"chunk {_}, generating fake emails for this chunk", colour="red")}
        with open(f"fakeemails.txt", "a+") as f:
            f.writelines(tempset)
    return


def main() -> None:
    numoflines: int = int(
        input("How many fake emails do you want to generate? "))
    populate_set_and_save_chunks_of_size(numoflines)


if __name__ == "__main__":
    main()

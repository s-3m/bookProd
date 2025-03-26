def filtering_cover(text: str):
    cover = [
        ("Книжка-картонка", "Картон"),
        ("Мягкая", "Мягкая обложка"),
        ("На спирали", "Мягкая обложка, на спирали"),
        ("Пластиковая упаковка", "del"),
        ("Пластиковая коробка", "del"),
        ("Пластиковый бокс", "del"),
        ("Твёрдая", "Твердый переплет"),
    ]

    for i in cover:
        if i[0] in text:
            return i[1]
    return text

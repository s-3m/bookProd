def filtering_cover(text: str):
    cover = [
        ("BB", "Мягкая обложка"),
        ("BC", "Мягкая обложка"),
        ("BG", "Мягкая обложка"),
        ("CA", "Мягкая обложка"),
        ("ZA", "Мягкая обложка"),
        ("Карта", "Мягкая обложка"),
        ("Карта в плоском исполнении", "Мягкая обложка"),
        ("Интегральный", "Мягкая обложка"),
        ("Карта, свернутая", "Мягкая обложка"),
        ("Карта, сложенная", "Мягкая обложка"),
        ("Карточки", "Мягкая обложка"),
        ("Книга", "Мягкая обложка"),
        ("Обложка", "Мягкая обложка"),
        ("Кожа/ Улучшенный переплет", "Кожаный переплет"),
        ("Спиральный переплет", "Картон на спирали"),
        ("Твёрдый переплёт", "Твердый переплет"),
    ]

    for i in cover:
        if i[0] in text:
            return i[1]
    return text
export const navItems = [
  { id: "dashboard", label: "Дашборд", icon: "grid", path: "/" },
  { id: "inbox", label: "Входящие", icon: "inbox", path: "/inbox" },
  { id: "vacancies", label: "Вакансии", icon: "briefcase", path: "/vacancies" },
  { id: "bench", label: "Бенч", icon: "users", path: "/bench" },
  { id: "matches", label: "Совпадения", icon: "link", path: "/matches" },
  { id: "run", label: "Ручной прогон", icon: "bolt", path: "/process" },
  { id: "logs", label: "Логи", icon: "wave", path: "/logs" },
  { id: "settings", label: "Настройки", icon: "gear", path: "/settings" }
];

export const dashboardStats = [
  { label: "Новых сообщений", value: "20", accent: "purple" },
  { label: "Активных вакансий", value: "9", accent: "blue" },
  { label: "Специалистов в бенче", value: "9", accent: "purple" },
  { label: "Совпадений на проверке", value: "6", accent: "gold" }
];

export const inboxItems = [
  ["Вакансия", "IT Вакансии Москва", "@user_0", "Ищем Senior Java Developer в МТС! Стек: Java 17+, Spring Boot, Kafka, PostgreSQL.", "Активно", "05 мар. 2026, 12:29"],
  ["Бенч", "Python разработчики", "@user_1", "На бенче Senior Python Developer A.K., опыт 6 лет. Стек: Python, Django, FastAPI.", "Активно", "04 мар. 2026, 10:29"],
  ["Бенч", "QA & Testing", "@user_14", "Team Lead Python на бенче E.M., 8 лет опыта Python, FastAPI, Django.", "Активно", "04 мар. 2026, 08:29"],
  ["Вакансия", "Java Jobs", "@user_2", "ВТБ ищет Middle React Developer. Требования: React 18+, TypeScript.", "Активно", "03 мар. 2026, 08:29"],
  ["Другое", "IT Вакансии Москва", "@user_15", "СТОП по вакансии Java Senior в МТС — позиция закрыта!", "Закрыто", "03 мар. 2026, 06:29"],
  ["Бенч", "Frontend разработка", "@user_3", "Свободен iOS Developer M.C., Middle+, 4 года опыта Swift, UIKit.", "Активно", "02 мар. 2026, 06:29"],
  ["Вакансия", "Python разработчики", "@user_16", "Ищем Junior+ React Developer. React, JavaScript, HTML/CSS опыт от 1 года.", "Активно", "02 мар. 2026, 04:29"]
];

export const vacancies = [
  ["МТС", ["Spring Boot", "Java", "PostgreSQL"], "Senior", "450 000 ₽", "Активно", "05 мар. 2026"],
  ["ВТБ", ["TypeScript", "React"], "Middle", "350 000 ₽", "Активно", "03 мар. 2026"],
  ["Не указан", ["PyTorch", "Python", "React", "Java"], "Junior", "180 000 ₽", "Активно", "02 мар. 2026"],
  ["Сбер", ["Docker", "Kubernetes"], "Middle", "500 000 ₽", "Активно", "01 мар. 2026"],
  ["Ozon", ["PyTorch", "Python"], "Middle", "600 000 ₽", "Активно", "28 фев. 2026"],
  ["Яндекс", ["PyTorch", "Python", "SQL"], "Middle", "300 000 ₽", "Активно", "27 фев. 2026"],
  ["VK", ["Python", "PostgreSQL"], "Middle", "420 000 ₽", "Активно", "24 фев. 2026"],
  ["Тинькофф", ["SQL"], "Middle", "350 000 ₽", "Активно", "21 фев. 2026"],
  ["Газпром нефть", ["Python"], "Senior", "550 000 ₽", "Активно", "19 фев. 2026"]
];

export const benchItems = [
  ["А.К.", ["Django", "Python", "FastAPI", "PostgreSQL"], "Senior", "400 000 ₽", "Москва", "Активно", "04 мар. 2026"],
  ["Е.М.", ["MongoDB", "Django", "Python", "FastAPI"], "Lead", "550 000 ₽", "Удалёнка", "Активно", "04 мар. 2026"],
  ["М.С.", ["Swift"], "Middle", "350 000 ₽", "Удалёнка", "Активно", "02 мар. 2026"],
  ["А.П.", ["Docker", "Go", "Kubernetes", "PostgreSQL"], "Middle", "380 000 ₽", "Москва", "Активно", "01 мар. 2026"],
  ["П.В.", ["Long-term", "Selenium", "Java"], "Senior", "380 000 ₽", "Удалёнка", "Активно", "28 фев. 2026"],
  ["И.К.", [], "Middle", "400 000 ₽", "Удалёнка", "Активно", "26 фев. 2026"],
  ["К.Н.", ["Kotlin", "Java"], "Middle", "—", "Москва", "Активно", "25 фев. 2026"],
  ["С.А.", [".NET"], "Senior", "450 000 ₽", "Удалёнка", "Активно", "22 фев. 2026"],
  ["Д.О.", ["TypeScript", "React", "Next.js"], "Middle", "320 000 ₽", "Удалёнка", "Активно", "20 фев. 2026"]
];

export const matches = [
  {
    score: 40,
    title: "Совпадение по стеку: PostgreSQL",
    vacancy: { company: "VK", stack: ["Python", "PostgreSQL"], grade: "Middle", rate: "420 000 ₽" },
    candidate: { name: "А.П.", stack: ["Docker", "Go", "Kubernetes"], grade: "Middle", rate: "380 000 ₽" },
    status: "На проверке",
    date: "Найдено: 05 мар. 2026, 12:29"
  },
  {
    score: 48,
    title: "Совпадение по стеку: Python, PostgreSQL",
    vacancy: { company: "Яндекс", stack: ["PyTorch", "Python", "SQL"], grade: "Middle", rate: "300 000 ₽" },
    candidate: { name: "А.К.", stack: ["Django", "Python", "FastAPI"], grade: "Senior", rate: "400 000 ₽" },
    status: "На проверке",
    date: "Найдено: 04 мар. 2026, 10:29"
  }
];

export const logs = [
  ["OK", "gpt-5.2", "daf8b8ea...", "150", "05 мар. 2026, 12:29:06"],
  ["OK", "gpt-5.2", "c702d05d...", "200", "05 мар. 2026, 11:29:06"],
  ["OK", "gpt-5.2", "dd9c8929...", "250", "05 мар. 2026, 10:29:06"],
  ["Ошибка", "gpt-5.2", "50cce54c...", "300", "05 мар. 2026, 09:29:06"],
  ["OK", "gpt-5.2", "cbad20be...", "350", "05 мар. 2026, 08:29:06"],
  ["OK", "gpt-5.2", "89941408...", "400", "05 мар. 2026, 07:29:06"],
  ["OK", "gpt-5.2", "51146d01...", "450", "05 мар. 2026, 06:29:06"],
  ["Ошибка", "gpt-5.2", "d713746e...", "500", "05 мар. 2026, 05:29:06"]
];

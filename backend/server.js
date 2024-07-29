const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const path = require('path');
const moment = require('moment');
const cors = require('cors'); // Importar el paquete cors



const app = express();
const upload = multer({ dest: 'uploads/' });

// Usar el middleware CORS
app.use(cors());


// Conectar a la base de datos SQLite en un archivo
const db = new sqlite3.Database('./database.db');

// Crear tablas si no existen
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS books (
    isbn13 TEXT PRIMARY KEY, 
    ISBN13_guiones TEXT, 
    titulo TEXT, 
    autor TEXT, 
    editorial TEXT, 
    sello TEXT, 
    texto_bic_materia_destacada TEXT
  )`);
  
  db.run(`CREATE TABLE IF NOT EXISTS sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    isbn13 TEXT,
    fecha TEXT,
    ventas INTEGER,
    FOREIGN KEY(isbn13) REFERENCES books(isbn13)
  )`);
});

// Endpoint para subir archivo de libros
app.post('/upload', upload.single('file'), (req, res) => {
  const filePath = req.file.path;

  fs.createReadStream(filePath)
    .pipe(csv({ separator: ';' }))
    .on('data', (row) => {
      const { isbn13, ISBN13_guiones, titulo, autor, editorial, sello, texto_bic_materia_destacada } = row;
      db.run("INSERT INTO books (isbn13, ISBN13_guiones, titulo, autor, editorial, sello, texto_bic_materia_destacada) VALUES (?, ?, ?, ?, ?, ?, ?)", [isbn13, ISBN13_guiones, titulo, autor, editorial, sello, texto_bic_materia_destacada], function(err) {
        if (err) {
          return console.log(err.message);
        }
        console.log(`A row has been inserted with rowid ${this.lastID}`);
      });
    })
    .on('end', () => {
      fs.unlinkSync(filePath);
      res.send('Archivo CSV de libros procesado y datos guardados en la base de datos');
    });
});

// Endpoint para subir archivo de ventas
app.post('/upload-sales', upload.single('file'), (req, res) => {
  const filePath = req.file.path;

  fs.createReadStream(filePath)
    .pipe(csv({ separator: ';' })) // El archivo de ventas usa punto y coma como delimitador
    .on('data', (row) => {
      const { isbn13, fecha, ventas } = row;
      const formattedDate = moment(fecha, 'DD-MM-YYYY').format('YYYY-MM-DD');
      if (!moment(formattedDate, 'YYYY-MM-DD', true).isValid()) {
        console.error(`Invalid date format for row: ${JSON.stringify(row)}`);
        return;
      }
      db.run("INSERT INTO sales (isbn13, fecha, ventas) VALUES (?, ?, ?)", [isbn13, formattedDate, ventas], function(err) {
        if (err) {
          return console.log(err.message);
        }
        console.log(`A row has been inserted with rowid ${this.lastID}`);
      });
    })
    .on('end', () => {
      fs.unlinkSync(filePath);
      res.send('Archivo CSV de ventas procesado y datos guardados en la base de datos');
    });
});

// Endpoint para obtener los datos de los libros y sus ventas
app.get('/books', (req, res) => {
  const query = `
    SELECT books.*, IFNULL(SUM(sales.ventas), 0) AS total_sales
    FROM books
    LEFT JOIN sales ON books.isbn13 = sales.isbn13
    GROUP BY books.isbn13
  `;

  db.all(query, (err, rows) => {
    if (err) {
      res.status(500).send(err.message);
      return;
    }
    res.json(rows);
  });
});

// Funciones para calcular rankings
const getSalesRanking = (period, category, res, value) => {
  let dateCondition;
  if (period === 'weekly') {
    dateCondition = `strftime('%Y-%W', fecha) = '${value}'`;
  } else if (period === 'monthly') {
    dateCondition = `strftime('%Y-%m', fecha) = '${value}'`;
  } else if (period === 'yearly') {
    dateCondition = "strftime('%Y', fecha) = strftime('%Y', 'now')";
  }

  let groupByField;
  if (category === 'author') {
    groupByField = 'books.autor';
  } else if (category === 'editorial') {
    groupByField = 'books.editorial';
  } else if (category === 'materia_destacada') {
    groupByField = 'books.texto_bic_materia_destacada';
  } else {
    groupByField = 'books.isbn13, books.titulo';
  }

  const query = `
    SELECT ${groupByField} AS category, SUM(sales.ventas) as total_sales
    FROM sales
    JOIN books ON sales.isbn13 = books.isbn13
    WHERE ${dateCondition}
    GROUP BY category
    ORDER BY total_sales DESC
    LIMIT 10
  `;

  console.log(`Executing query for ${period} ${category} ranking:`);
  console.log(query);

  db.all(query, (err, rows) => {
    if (err) {
      res.status(500).send(err.message);
      return;
    }
    console.log(`Ranking results for ${period} ${category}:`);
    console.log(rows);
    res.json(rows);
  });
};

// Endpoint para obtener el ranking de ventas semanal
app.get('/sales-ranking/weekly/:week/:category', (req, res) => {
  const week = req.params.week;
  const category = req.params.category;
  getSalesRanking('weekly', category, res, week);
});

// Endpoint para obtener el ranking de ventas mensual
app.get('/sales-ranking/monthly/:month/:category', (req, res) => {
  const month = req.params.month;
  const category = req.params.category;
  getSalesRanking('monthly', category, res, month);
});

// Endpoint para obtener el ranking de ventas anual
app.get('/sales-ranking/yearly/:category', (req, res) => {
  const category = req.params.category;
  getSalesRanking('yearly', category, res);
});

// Endpoint para obtener todas las semanas disponibles en la base de datos
app.get('/weeks', (req, res) => {
  const query = `
    SELECT DISTINCT strftime('%Y-%W', fecha) as week
    FROM sales
    ORDER BY week
  `;

  db.all(query, (err, rows) => {
    if (err) {
      res.status(500).send(err.message);
      return;
    }
    res.json(rows);
  });
});

// Endpoint para obtener todos los meses disponibles en la base de datos
app.get('/months', (req, res) => {
  const query = `
    SELECT DISTINCT strftime('%Y-%m', fecha) as month
    FROM sales
    ORDER BY month
  `;

  db.all(query, (err, rows) => {
    if (err) {
      res.status(500).send(err.message);
      return;
    }
    res.json(rows);
  });
});




// Endpoint para obtener ventas mensuales para un libro en un año específico
app.get('/monthly-sales/book/:isbn13/:year', (req, res) => {
  const { isbn13, year } = req.params;

  const query = `
    SELECT strftime('%Y-%m', fecha) as month, SUM(ventas) as total_sales
    FROM sales
    WHERE isbn13 = ? AND strftime('%Y', fecha) = ?
    GROUP BY month
    ORDER BY month
  `;

  db.all(query, [isbn13, year], (err, rows) => {
    if (err) {
      res.status(500).send(err.message);
      return;
    }
    res.json(rows);
  });
});

// Endpoint para obtener ventas semanales para un libro en un mes específico
app.get('/weekly-sales/book/:isbn13/:month', (req, res) => {
  const { isbn13, month } = req.params;

  const query = `
    SELECT strftime('%Y-%W', fecha) as week, SUM(ventas) as total_sales
    FROM sales
    WHERE isbn13 = ? AND strftime('%Y-%m', fecha) = ?
    GROUP BY week
    ORDER BY week
  `;

  db.all(query, [isbn13, month], (err, rows) => {
    if (err) {
      res.status(500).send(err.message);
      return;
    }
    res.json(rows);
  });
});

// Endpoint para obtener ventas semanales para un autor en un mes específico
app.get('/weekly-sales/author/:author/:month', (req, res) => {
  const { author, month } = req.params;

  const query = `
    SELECT strftime('%Y-%W', fecha) as week, SUM(ventas) as total_sales
    FROM sales
    JOIN books ON sales.isbn13 = books.isbn13
    WHERE books.autor = ? AND strftime('%Y-%m', fecha) = ?
    GROUP BY week
    ORDER BY week
  `;

  db.all(query, [author, month], (err, rows) => {
    if (err) {
      res.status(500).send(err.message);
      return;
    }
    res.json(rows);
  });
});

// Endpoint para obtener ventas mensuales para un autor en un año específico
app.get('/monthly-sales/author/:author/:year', (req, res) => {
  const { author, year } = req.params;

  const query = `
    SELECT strftime('%Y-%m', fecha) as month, SUM(ventas) as total_sales
    FROM sales
    JOIN books ON sales.isbn13 = books.isbn13
    WHERE books.autor = ? AND strftime('%Y', fecha) = ?
    GROUP BY month
    ORDER BY month
  `;
  db.all(query, [author, year], (err, rows) => {
    if (err) {
      res.status(500).send(err.message);
      return;
    }
    res.json(rows);
  });
});

// Endpoint para obtener ventas semanales para una editorial en un mes específico
app.get('/weekly-sales/editorial/:editorial/:month', (req, res) => {
  const { editorial, month } = req.params;
  const decodedEditorial = decodeURIComponent(editorial);

  const query = `
    SELECT strftime('%Y-%W', fecha) as week, SUM(ventas) as total_sales
    FROM sales
    JOIN books ON sales.isbn13 = books.isbn13
    WHERE books.editorial = ? AND strftime('%Y-%m', fecha) = ?
    GROUP BY week
    ORDER BY week
  `;

  db.all(query, [decodedEditorial, month], (err, rows) => {
    if (err) {
      res.status(500).send(err.message);
      return;
    }
    res.json(rows);
  });
});


// Endpoint para obtener ventas mensuales para una editorial en un año específico
app.get('/monthly-sales/editorial/:editorial/:year', (req, res) => {
  const { editorial, year } = req.params;
  const decodedEditorial = decodeURIComponent(editorial);

  const query = `
    SELECT strftime('%Y-%m', fecha) as month, SUM(ventas) as total_sales
    FROM sales
    JOIN books ON sales.isbn13 = books.isbn13
    WHERE books.editorial = ? AND strftime('%Y', fecha) = ?
    GROUP BY month
    ORDER BY month
  `;

  db.all(query, [decodedEditorial, year], (err, rows) => {
    if (err) {
      res.status(500).send(err.message);
      return;
    }
    res.json(rows);
  });
});


// Endpoint para obtener ventas semanales para una materia destacada en un mes específico
app.get('/weekly-sales/materia/:materia/:month', (req, res) => {
  const { materia, month } = req.params;
  const decodedMateria = decodeURIComponent(materia);

  const query = `
    SELECT strftime('%Y-%W', fecha) as week, SUM(ventas) as total_sales
    FROM sales
    JOIN books ON sales.isbn13 = books.isbn13
    WHERE books.texto_bic_materia_destacada = ? AND strftime('%Y-%m', fecha) = ?
    GROUP BY week
    ORDER BY week
  `;

  db.all(query, [decodedMateria, month], (err, rows) => {
    if (err) {
      res.status(500).send(err.message);
      return;
    }
    res.json(rows);
  });
});

// Endpoint para obtener las ventas mensuales de una materia destacada en un año específico
app.get('/monthly-sales/materia/:category/:year', (req, res) => {
    const category = req.params.category;
    const year = req.params.year;
  
    const query = `
      SELECT strftime('%Y-%m', fecha) as month, SUM(ventas) as total_sales
      FROM sales
      JOIN books ON sales.isbn13 = books.isbn13
      WHERE books.texto_bic_materia_destacada = ? AND strftime('%Y', fecha) = ?
      GROUP BY month
      ORDER BY month
    `;
  
    db.all(query, [category, year], (err, rows) => {
      if (err) {
        res.status(500).send(err.message);
        return;
      }
      res.json(rows);
    });
  });
  

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

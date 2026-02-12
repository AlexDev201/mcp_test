import os
from typing import List, Dict,Any,  Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from fastmcp import FastMCP

app = FastMCP("company-db-server")

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATA"),
        cursor_factory = RealDictCursor
    )

    return conn

@app.tool
def list_employees(limit: int = 5) -> List[Dict[str, Any]]: 
    """"Listar los empleados """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(""""
                       SELECT id, name, position, departament, salary, hire_date 
                       FROM employees
                       ORDER BY id
                       LIMIT %s
                       """,(limit,) )
        
        rows = cursor.fetchall()
        employees = []
        for row in rows:
            employees.append({ 
                "id": row["id"],
                "name": row["name"],
                "position": row["position"],
                "departament": row["departament"],
                "salary": float(row["salary"]),
                "hire_date": str(row["hire_date"].isoformat())
            })

            cursor.close()
            conn.close()

            return employees
        
    except Exception as e:
        return {f"Error al listar empleados: {str(e)}"}
    
@app.tool
def add_employee(name: str, position: str, department: str, salary: float, hire_date: Optional[str] = None):
    """Agregar un nuevo empleado"""
    try:
        if not name.strip():
            return {"error": "El nombre es requerido"}

        if salary <= 0:
            return {"error": "El salario debe ser mayor a 0"}

        if not hire_date:
            hire_date = datetime.now().strftime('%Y-%m-%d')

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO employees(name, position, department, salary, hire_date)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, name, position, department, salary, hire_date
            """,
            (name.strip(), position.strip(), department.strip(), salary, hire_date)
        )

        new_employee = cursor.fetchone()
        conn.commit()

        cursor.close()
        conn.close()

        return {
            "success": True,
            "employee": {
                "id": new_employee[0],
                "name": new_employee[1],
                "position": new_employee[2],
                "department": new_employee[3],
                "salary": float(new_employee[4]),
                "hire_date": new_employee[5]
            }
        }
    
    except Exception as e:
        return{"error" : f"Error al agregar empleado: {str(e)} "}


if __name__ == "__main__":
    app.run(transport="sse", host = "0.0.0.0", port = 3000)
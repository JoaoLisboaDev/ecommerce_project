-- Verificar qual o meu utilizador atual
SELECT CURRENT_USER();

-- Verificar as permissões do utilizador atual
SHOW GRANTS FOR 'root'@'localhost';

-- Validar que o utilizador foi criado corretamente
SELECT User, Host, plugin, password_expired, account_locked
FROM mysql.user
WHERE User = 'data_analyst';
use crate::conductor::Conductor;
use crate::database_state::DatabaseState;
use crate::server_state::ServerState;

use common::commands::{Command, DBCommand, Response, SystemCommand};

use common::error::c_err;
use common::{CrustyError, QueryResult};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub fn handle_command(
    shutdown_signal: Arc<AtomicBool>,
    quiet_mode: &mut bool,
    command: Command,
    server_state: &'static ServerState,
    client_id: u64,
) -> (bool, Response) {
    info!("Handling command: {:?}", command);
    match command {
        Command::System(system_command) => handle_system_command(
            shutdown_signal,
            quiet_mode,
            client_id,
            server_state,
            system_command,
        ),
        Command::DB(database_command) => {
            if let Ok(db) = server_state.get_connected_db(client_id) {
                handle_database_command(db, database_command)
            } else {
                error!("Client {} is not connected to a database", client_id);
                (
                    false,
                    Response::SystemErr("Not connected to a database".to_string()),
                )
            }
        }
    }
}

pub fn handle_system_command(
    shutdown_signal: Arc<AtomicBool>,
    quiet_mode: &mut bool,
    client_id: u64,
    server_state: &'static ServerState,
    system_command: SystemCommand,
) -> (bool, Response) {
    let response = run_system_command(
        shutdown_signal,
        quiet_mode,
        client_id,
        server_state,
        system_command.clone(),
    );

    match response {
        Ok(response) => response,
        Err(e) => (false, Response::SystemErr(e.to_string())),
    }
}

fn run_system_command(
    shutdown_signal: Arc<AtomicBool>,
    quiet_mode: &mut bool,
    client_id: u64,
    server_state: &'static ServerState,
    system_command: SystemCommand,
) -> Result<(bool, Response), CrustyError> {
    match system_command {
        SystemCommand::ShowDatabases => {
            let db_names = server_state.get_db_names();
            let response = Response::SystemMsg(format!("Databases: {}", db_names.join(", ")));
            Ok((false, response))
        }
        SystemCommand::Reset => {
            server_state.reset()?;
            let response = Response::SystemMsg("Reset server.".to_string());
            Ok((false, response))
        }
        SystemCommand::Shutdown => {
            shutdown_signal.store(true, std::sync::atomic::Ordering::Release);
            server_state.shutdown()?;
            let response = Response::Shutdown;
            Ok((true, response))
        }
        SystemCommand::QuietMode => {
            *quiet_mode = true;
            Ok((false, Response::QuietOk))
        }
        SystemCommand::Test => {
            unimplemented!()
        }
        SystemCommand::Create(db_name) => {
            server_state.create_new_db(&db_name)?;
            let response = Response::SystemMsg(format!("Created database {}", db_name));
            Ok((false, response))
        }
        SystemCommand::Connect(db_name) => {
            server_state.connect_to_db(&db_name, client_id)?;
            let response = Response::SystemMsg(format!("Connected to database {}", db_name));
            Ok((false, response))
        }
        SystemCommand::CloseConnection => {
            server_state.close_connection(client_id);
            let response = Response::SystemMsg("Closing connection".to_string());
            Ok((false, response))
        }
    }
}

pub fn handle_database_command(
    db: &'static DatabaseState,
    database_command: DBCommand,
) -> (bool, Response) {
    match run_database_command(db, database_command.clone()) {
        Ok(response) => response,
        Err(e) => (false, Response::QueryExecutionError(e.to_string())),
    }
}

pub fn run_database_command(
    db: &'static DatabaseState,
    database_command: DBCommand,
) -> Result<(bool, Response), CrustyError> {
    match database_command {
        DBCommand::ExecuteSQL(sql) => {
            let mut conductor = Conductor::new(db.managers)?;
            let qr = conductor.run_sql_from_string(sql, db)?;
            Ok((false, Response::QueryResult(qr)))
        }
        DBCommand::ShowTables => {
            let tables = db.get_table_names()?;
            let result = QueryResult::MessageOnly(format!("Tables: {}", tables.join(", ")));
            Ok((false, Response::QueryResult(result)))
        }
        DBCommand::Import(table_name, file_path) => {
            let mut conductor = Conductor::new(db.managers)?;
            let qr = conductor.import_csv(&table_name, file_path, db)?;
            if let QueryResult::Insert {
                inserted,
                table_name,
            } = qr
            {
                let response = Response::SystemMsg(format!(
                    "Imported {} rows into table {}",
                    inserted, table_name
                ));
                Ok((false, response))
            } else {
                Err(c_err("Unexpected query result from import"))
            }
        }
        DBCommand::ShowQueries => {
            unimplemented!()
        }
        DBCommand::RegisterQuery(_query) => {
            unimplemented!()
        }
        DBCommand::Generate(_file_name) => {
            unimplemented!()
        }
        DBCommand::ConvertQuery(_sql) => {
            unimplemented!()
        }
        DBCommand::RunQueryFull(_query) => {
            unimplemented!()
        }
        DBCommand::RunQueryPartial(_query) => {
            unimplemented!()
        }
    }
}
